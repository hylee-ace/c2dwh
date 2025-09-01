import httpx, json, asyncio, os, re
from .crawler import Crawler
from .models import Product, Watch, Phone, Laptop, Screen, Tablet, Earphone
from utils import colorized, dict_to_csv, s3_file_uploader
from py_mini_racer import MiniRacer
from urllib.parse import urlparse
from datetime import datetime


class Scraper:
    """
    Asynchronous URL scraper that fetches desired information from HTML contents.

    Attributes
    ----------
    urls: list[str]
        The list of URLs waiting for being scraped.
    target:str
        The website that going to be scraped, currently support **cellphones** and **tgdd**
    save_in: str, optional
        Directory for saved output (default: **None**).
    """

    __retailer = None
    saving_dir = None
    __saving_paths = dict()  # inclust path and is_removed status (1 and 0)
    __queue = set()
    __scraped = set()
    result = list()
    upload_to_s3 = False
    s3_attrs = dict()
    __lock = asyncio.Lock()

    def __init__(
        self,
        urls: list[str],
        *,
        save_in: str = None,
        upload_to_s3: bool = False,
        s3_attrs: dict | None = None,
    ):
        Scraper.__queue.update(urls)
        Scraper.__retailer = "".join(
            [
                i
                for i in urlparse(urls[0]).hostname.split(".")
                if i not in ["com", "vn", "www"]
            ]
        ).upper()

        if save_in:
            Scraper.saving_dir = save_in

        if upload_to_s3:
            if not save_in:
                print(
                    "Cannot locate output file for uploading since 'save_in' is missing."
                )
                exit(1)
            if not s3_attrs:  # not provide s3 attributes
                print(
                    "S3 attributes such as client (optional), bucket and obj_prefix are required."
                )
                exit(1)
            if not all(
                [
                    True if i in ["client", "bucket", "obj_prefix"] else False
                    for i in s3_attrs.keys()
                ]
            ):  # giving invalid attrs
                print("Invalid S3 attributes.")
                exit(1)

            Scraper.upload_to_s3 = upload_to_s3
            Scraper.s3_attrs = s3_attrs

    @staticmethod
    async def nuxt_to_data(
        url: str,
        *,
        client: httpx.AsyncClient = None,
        semaphore: asyncio.Semaphore = None,
        encoding: str = None,
    ):
        """
        Extract data from Nuxt.js-based HTML by JS runner.
        """

        nuxt = await Crawler.async_inspect(
            url,
            client=client,
            xpath="//script[not(@*) and contains(.,'window.__NUXT__')]/text()",
            semaphore=semaphore,
            encoding=encoding,
        )

        if not nuxt:
            return

        # prepare JS runner
        js_runner = MiniRacer()
        js_runner.eval("var window = {};")
        js_runner.eval(nuxt[0])
        data = js_runner.eval("JSON.stringify(window.__NUXT__)")

        # load to json
        return json.loads(data)

    def __parse_common_info(data: dict):
        prd = Product(
            product_id=data["sku"].strip(),
            name=data["name"].strip(),
            price=int(data["offers"]["price"]),
            brand=data["brand"]["name"][0].strip(),
            url=data["url"].strip(),
        )

        if data["aggregateRating"]:
            prd.rating = data["aggregateRating"]["ratingValue"]
            prd.reviews_count = int(data["aggregateRating"]["reviewcount"])

        released_value = [
            i["value"].strip()
            for i in data["additionalProperty"]
            if i["name"] == "Thời điểm ra mắt"
            or i["name"] == "Thời gian ra mắt"
            or i["name"] == "Năm ra mắt"
        ]
        prd.release_date = released_value[0] if released_value else None

        # check device type
        weight_value = [
            i["value"].split(" - ")[-1].strip()
            for i in data["additionalProperty"]
            if i["name"] == "Kích thước, khối lượng" or i["name"] == "Khối lượng"
        ]
        jack_value = [
            i["value"].strip()
            for i in data["additionalProperty"]
            if i["name"] == "Jack cắm"
        ]

        if prd.url.split("/")[3] == "laptop":  # classify by url hint
            prd.category = "Laptop"
        elif prd.url.split("/")[3] == "may-tinh-bang":
            prd.category = "Tablet"
        elif prd.url.split("/")[3] == "man-hinh-may-tinh":
            prd.category = "Screen"
        else:  # classify by weight
            if weight_value:
                g_vals = re.findall(r"(\d+\.?\d*)\s?(?:g|\()", weight_value[0])
                gam = float(g_vals[0]) if g_vals else None  # actual weight value in gam

                if prd.url.split("/")[3] == "dtdd":
                    prd.category = "Smartphone" if gam and gam >= 135.0 else "Phone"
                elif prd.url.split("/")[3] == "dong-ho-thong-minh":
                    prd.category = "Smartwatch" if gam and gam >= 30.0 else "Smartband"
                else:
                    prd.category = (
                        "Headphone"
                        if gam and gam > 100.0
                        else "Earphone" if jack_value else "Earbuds"
                    )

        return {
            "product_id": prd.product_id,
            "name": prd.name,
            "price": prd.price,
            "brand": prd.brand,
            "category": prd.category,
            "rating": prd.rating,
            "reviews_count": prd.reviews_count,
            "url": prd.url,
            "release_date": prd.release_date,
        }

    def __parse_specs_info(data: dict, type: str):
        def cpu():  # for laptop, phone, watch, tablet
            cpu_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Công nghệ CPU", "CPU", "Chip xử lý (CPU)"]
            ]
            return re.sub(r"</?a.*?>", "", cpu_value[0]).strip() if cpu_value else None

        def cpuspeed():  # for laptop, tablet
            cpuspd_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Tốc độ CPU"
            ]
            return cpuspd_value[0] if cpuspd_value else None

        def gpu(device: str):  # for laptop, tablet
            gpu_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Card màn hình", "Chip đồ hoạ (GPU)"]
            ]
            if device == "laptop":
                return (
                    re.sub(r"</?a.*?>", "", gpu_value[0]).strip() if gpu_value else None
                )
            return gpu_value[0] if gpu_value else None

        def ram():  # for laptop, phone, tablet, watch
            ram_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "RAM"
            ]
            return ram_value[0] if ram_value else None

        def ramtype():  # for laptop
            ramtype_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Loại RAM", "Tốc độ Bus RAM"]
            ]
            return " ".join(ramtype_value) if ramtype_value else None

        def disk():  # for laptop, phone, tablet, watch
            disk_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Ổ cứng", "Dung lượng lưu trữ", "Bộ nhớ trong"]
            ]
            return disk_value[0] if disk_value else None

        def screensize(device: str):  # for laptop, phone, tablet, watch, screen
            scsize_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Màn hình rộng", "Kích thước màn hình"]
            ]
            if device in ["phone", "tablet"]:
                return (
                    re.findall(r'\d\.?\d*\s?"', scsize_value[0])[0]
                    if scsize_value and re.findall(r'\d\.?\d*\s?"', scsize_value[0])
                    else None
                )
            return scsize_value[0] if scsize_value else None

        def screentech():  # for laptop, phone, tablet, watch, screen
            sctech_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Công nghệ màn hình", "Tấm nền"]
            ]
            return sctech_value[0] if sctech_value else None

        def screenres():  # for laptop, phone, tablet, screen
            scres_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Độ phân giải màn hình", "Độ phân giải"]
            ]
            return scres_value[0] if scres_value else None

        def screenfreq(device: str):  # for laptop, phone, tablet, screen
            scfreq_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Màn hình rộng", "Tần số quét"]
            ]
            if device in ["phone", "tablet"]:
                return (
                    re.findall(r">?\s*(\d+\s*Hz)\s*<?", scfreq_value[0])[0]
                    if scfreq_value
                    and re.findall(r">?\s*(\d+\s*Hz)\s*<?", scfreq_value[0])
                    else None
                )

            return scfreq_value[0] if scfreq_value else None

        def optsys():  # for laptop, phone, tablet, watch
            os_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Hệ điều hành"
            ]
            return os_value[0] if os_value else None

        def battery():  # for laptop, phone, tablet, watch, earphone
            battery_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"]
                in ["Thông tin Pin", "Dung lượng pin", "Thời lượng pin tai nghe"]
            ]
            return battery_value[0] if battery_value else None

        def case_battery():  # for earphone
            casebtr_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Thời lượng pin hộp sạc"
            ]
            return casebtr_value[0] if casebtr_value else None

        def connectivity():  # for watch, earphone
            cnt_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] in ["Kết nối", "Công nghệ kết nối"]
            ]
            return cnt_value[0] if cnt_value else None

        def control():  # for earphone
            ctr_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Điều khiển"
            ]
            return ctr_value[0] if ctr_value else None

        def powercsp():  # for screen
            pwcsp_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Công suất tiêu thụ điện"
            ]
            return pwcsp_value[0] if pwcsp_value else None

        def ports():  # for screen
            ports_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Cổng kết nối"
            ]
            return ports_value[0] if ports_value else None

        if type == "phone":
            return {
                "chipset": cpu(),
                "ram": ram(),
                "storage": disk(),
                "scr_size": screensize("phone"),
                "scr_tech": screentech(),
                "scr_res": screenres(),
                "scr_freq": screenfreq("phone"),
                "os": optsys(),
                "battery": battery(),
            }
        elif type == "laptop":
            return {
                "cpu": cpu(),
                "cpu_speed": cpuspeed(),
                "gpu": gpu("laptop"),
                "ram": ram(),
                "ram_type": ramtype(),
                "storage": disk(),
                "scr_size": screensize("laptop"),
                "scr_tech": screentech(),
                "scr_res": screenres(),
                "scr_freq": screenfreq("laptop"),
                "os": optsys(),
                "battery": battery(),
            }
        elif type == "tablet":
            return {
                "chipset": cpu(),
                "chipset_speed": cpuspeed(),
                "gpu": gpu("tablet"),
                "ram": ram(),
                "storage": disk(),
                "scr_size": screensize("tablet"),
                "scr_tech": screentech(),
                "scr_res": screenres(),
                "scr_freq": screenfreq("tablet"),
                "os": optsys(),
                "battery": battery(),
            }
        elif type == "watch":
            return {
                "chipset": cpu(),
                "storage": disk(),
                "scr_size": screensize("watch"),
                "scr_tech": screentech(),
                "os": optsys(),
                "connectivity": connectivity(),
                "battery": battery(),
            }
        elif type == "earphone":
            return {
                "connectivity": connectivity(),
                "battery": battery(),
                "case_battery": case_battery(),
                "control": control(),
            }
        else:
            return {
                "scr_size": screensize("screen"),
                "scr_tech": screentech(),
                "scr_res": screenres(),
                "scr_freq": screenfreq("screen"),
                "ports": ports(),
                "power_csp": powercsp(),
            }

    async def __scrape(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ):
        data = None
        product = None
        path = None

        data = await Crawler.async_inspect(
            url,
            client=client,
            xpath='//*[@id="productld"]/text()',
            semaphore=semaphore,
            encoding="utf-8",
        )

        # check if url were product page
        if not data:
            async with Scraper.__lock:
                Scraper.__scraped.add(url)
                Scraper.__queue.discard(url)
            return

        data = json.loads(data[0])

        # parse product info
        if url.split("/")[3] == "dtdd":
            product = Phone(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "phone"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_phones_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "laptop":
            product = Laptop(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "laptop"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_laptops_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "may-tinh-bang":
            product = Tablet(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "tablet"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_tablets_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "dong-ho-thong-minh":
            product = Watch(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "watch"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_watches_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "tai-nghe":
            product = Earphone(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "earphone"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_earphones_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "man-hinh-may-tinh":
            product = Screen(
                **Scraper.__parse_common_info(data),
                **Scraper.__parse_specs_info(data, "screen"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_screens_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0

        # update results
        async with Scraper.__lock:
            Scraper.__scraped.add(url)
            Scraper.__queue.discard(url)
            Scraper.result.append(product.info())

            if Scraper.saving_dir:
                if os.path.exists(path):  # remove duplicate files in a same date
                    if Scraper.__saving_paths[path] == 0 and os.path.getsize(path):
                        os.remove(path)
                        Scraper.__saving_paths.update({path: 1})  # update file status
                else:
                    Scraper.__saving_paths.update({path: 1})

                dict_to_csv(product.info(), path)

    @classmethod
    async def execute(
        cls,
        *,
        timeout: int | float = 10.0,
        follow_redirects: bool = True,
        headers: httpx.Headers = None,
        chunksize: int = 20,
        semaphore: asyncio.Semaphore = None,
        delay: float = None,
    ):
        """
        Start scraping process from given list of URLs.

        Parameters
        ---
        timeout: int, float, optional
            Request timeout in seconds (default: **10.0**).
        follow_redirects: bool, optional
            Whether to follow HTTP redirects (default: **True**).
        headers: httpx.Header, optional
            Custom HTTP request headers (default: **None**).
        chunksize: int, optional
            Number of URLs to process per batch. Be cautious, high request rate could lead to IP banned (default: **20**).
        semaphore: asyncio.Semaphore, optional
            Concurrency limit for simultaneous requests, best range in **5-20**.
        delay: float, optional
            Delay between chunks (default: **None**).
        """

        default_headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0",
            "Connection": "keep-alive",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8,en-GB;q=0.8",
            "Cache-Control": "no-cache",
            "Referer": "https://www.google.com/",
            "DNT": "1",  # not track request header
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Pragma": "no-cache",
        }

        if not headers:
            headers = default_headers
        else:
            copy = default_headers.copy()
            copy.update(headers)
            headers = copy

        if not semaphore:
            semaphore = asyncio.Semaphore(5)

        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=follow_redirects,
            headers=headers,
        ) as client:
            while cls.__queue:
                in_use = list(cls.__queue)[:chunksize]
                tasks = [
                    asyncio.create_task(cls.__scrape(i, client, semaphore))
                    for i in in_use
                ]

                await asyncio.gather(*tasks)

                if delay:
                    await asyncio.sleep(delay)  # delay between chunks

                print(
                    f"From: {cls.__retailer}",
                    f"Pending {len(cls.__queue)}",
                    f"Scraped: {len(cls.__scraped)}",
                    f"Valid: {len(cls.result)}",
                    sep=" | ",
                )

        print("Scraping successfully.")
        print(
            f"From: {colorized(cls.__retailer,34)}",
            f"Scraped: {colorized(len(cls.__scraped),33)}",
            f"Valid: {colorized(len(cls.result),32)}",
            sep=" | ",
        )

        # upload to s3 bucket
        if cls.upload_to_s3 and cls.s3_attrs:
            bucket = cls.s3_attrs["bucket"]
            files = []

            # uploading work
            def upload(file: str, bucket: str, key: str):
                fname = os.path.basename(file)
                print(f"Start uploading {fname} to {bucket}...")
                s3_file_uploader(
                    file, client=cls.s3_attrs.get("client"), bucket=bucket, key=key
                )
                print(f"Uploading {fname} successfully.")

            for i in Scraper.__saving_paths:
                filename = os.path.basename(i)
                key = f"{cls.s3_attrs['obj_prefix'] if cls.s3_attrs.get('obj_prefix') else ''}{filename}"
                date = datetime.fromisoformat(
                    filename.removesuffix(".csv").split("_")[-1]
                )
                if date.date() == datetime.now().date():  # check valid date
                    files.append({"file": i, "bucket": bucket, "key": key})

            tasks = [asyncio.to_thread(upload, **i) for i in files]
            await asyncio.gather(*tasks)

    @classmethod
    def reset(cls):
        """
        Reset scraper after use.
        """

        if cls.saving_dir:
            cls.saving_dir = None

        cls.__retailer = None
        cls.__queue.clear()
        cls.__scraped.clear()
        cls.result.clear()

        print(f"Scraper for {Scraper.__retailer} reset.")
