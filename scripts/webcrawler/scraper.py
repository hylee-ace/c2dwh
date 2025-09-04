import httpx, json, asyncio, os, re
from .crawler import Crawler
from .models import ProductInfo, Phone, Tablet, Laptop, Watch, Earphone, Screen
from utils import colorized, dict_to_csv, s3_file_uploader
from py_mini_racer import MiniRacer
from urllib.parse import urlparse
from datetime import datetime
from dataclasses import asdict


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
        prd = ProductInfo(
            sku=data["sku"].strip(),
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
        prd.release_date = released_value[0].strip() if released_value else None

        # check device type
        dim_value = [
            i["value"]
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
        else:  # classify by weight and width
            if dim_value:
                g_vals = re.findall(r"(\d+\.?\d*)\s?(?:g|\()", dim_value[0])
                gam = float(g_vals[0]) if g_vals else None  # actual weight value in gam

                if prd.url.split("/")[3] == "dtdd":
                    prd.category = "Smartphone" if gam and gam >= 135.0 else "Phone"
                elif prd.url.split("/")[3] == "dong-ho-thong-minh":
                    mm_vals = re.findall(r"Ngang\s?(\d+\.?\d*)\s?mm", dim_value[0])
                    mm = (
                        float(mm_vals[0]) if mm_vals else None
                    )  # actual width value in mm
                    prd.category = "Smartwatch" if mm and mm > 33.5 else "Smartband"
                else:
                    prd.category = (
                        "Headphone"
                        if gam and gam > 100.0
                        else "Earphone" if jack_value else "Earbuds"
                    )

        return asdict(prd)

    def __parse_specs_info(data: dict, device: str):
        def cpu():
            value = [
                j.strip()
                for i, j in data
                if i in ["Công nghệ CPU", "Chip xử lý (CPU)", "CPU"]
            ]
            return value[0] if value else None

        def cpu_cores():
            value = [j.strip() for i, j in data if i == "Số nhân"]
            return value[0] if value else None

        def cpu_threads():
            value = [j.strip() for i, j in data if i == "Số luồng"]
            return value[0] if value else None

        def cpu_speed():
            value = [j.strip() for i, j in data if i == "Tốc độ CPU"]
            return value[0] if value else None

        def gpu():
            value = [
                j.strip()
                for i, j in data
                if i in ["Chip đồ hoạ (GPU)", "Chip đồ họa (GPU)", "Card màn hình"]
            ]
            return value[0] if value else None

        def ram():
            value = [j.strip() for i, j in data if i == "RAM"]
            return value[0] if value else None

        def max_ram():
            value = [j.strip() for i, j in data if i == "Hỗ trợ RAM tối đa"]
            return value[0] if value else None

        def ram_type():
            value = [j.strip() for i, j in data if i == "Loại RAM"]
            return value[0] if value else None

        def ram_bus():
            value = [j.strip() for i, j in data if i == "Tốc độ Bus RAM"]
            return value[0] if value else None

        def storage():
            value = [
                j.strip()
                for i, j in data
                if i in ["Ổ cứng", "Dung lượng lưu trữ", "Bộ nhớ trong"]
            ]
            return value[0] if value else None

        def webcam():
            value = [j.strip() for i, j in data if i == "Webcam"]
            return value[0] if value else None

        def rearcam_specs():
            if device == "tablet":
                value = [j.strip() for i, j in data if i == "Độ phân giải"]
                return value[1] if value else None
            else:
                value = [j.strip() for i, j in data if i == "Độ phân giải camera sau"]
                return value[0] if value else None

        def frontcam_specs():
            if device == "tablet":
                value = [j.strip() for i, j in data if i == "Độ phân giải"]
                return value[-1] if value else None
            value = [j.strip() for i, j in data if i == "Độ phân giải camera trước"]
            return value[0] if value else None

        def screen_tech():
            value = [j.strip() for i, j in data if i == "Công nghệ màn hình"]
            return value[0] if value else None

        def screen_type():
            value = [
                j.strip()
                for i, j in data
                if i in ["Chất liệu mặt", "Mặt kính cảm ứng", "Loại màn hình"]
            ]
            return value[0] if value else None

        def screen_size():
            value = [
                j.strip()
                for i, j in data
                if i in ["Kích thước màn hình", "Màn hình rộng"]
            ]
            return value[0].split("-")[0].strip() if value else None

        def screen_panel():
            if device in ["laptop", "screen"]:
                value = [j.strip() for i, j in data if i == "Tấm nền"]
                return value[0] if value else None
            value = [j.strip() for i, j in data if i == "Công nghệ màn hình"]
            return value[0] if value else None

        def screen_res():
            value = [
                j.strip()
                for i, j in data
                if i in ["Độ phân giải", "Độ phân giải màn hình"]
            ]
            return value[0] if value else None

        def screen_rate():
            if device in ["screen", "laptop"]:
                value = [j.strip() for i, j in data if i == "Tần số quét"]
                return value[0] if value else None
            value = [j.strip() for i, j in data if i == "Màn hình rộng"]
            return re.sub(r".*\s(\d+\.?\d*\s*Hz)", r"\1", value[0]) if value else None

        def screen_nits():
            if device == "laptop":
                value = [j.strip() for i, j in data if i == "Công nghệ màn hình"]
                return (
                    re.sub(r".*\s+(\d+\s*nits).*", r"\1", value[0])
                    if value and re.findall(r"\d+\s?nits", value[0])
                    else None
                )
            value = [j.strip() for i, j in data if i == "Độ sáng tối đa"]
            return value[0] if value else None

        def os():
            value = [j.strip() for i, j in data if i == "Hệ điều hành"]
            return value[0] if value else None

        def water_resistant():
            if device == "earphone":
                value = [j.strip() for i, j in data if i == "Tiện ích"]
                return (
                    re.sub(r".*(IP\d+).*", r"\1", value[0])
                    if value and re.findall(r"IP\d+", value[0])
                    else None
                )
            value = [
                j.strip()
                for i, j in data
                if i in ["Chống nước / Kháng nước", "Kháng nước, bụi"]
            ]
            return value[0] if value else None

        def battery():
            value = [
                j.strip()
                for i, j in data
                if i in ["Thông tin Pin", "Dung lượng pin", "Thời lượng pin tai nghe"]
            ]
            return value[0] if value else None

        def charger():
            value = [j.strip() for i, j in data if i == "Hỗ trợ sạc tối đa"]
            return value[0] if value else None

        def weight():
            if device in ["laptop", "screen"]:
                value = [
                    j.strip()
                    for i, j in data
                    if i in ["Khối lượng có chân đế", "Kích thước"]
                ]
                return value[0].split("-")[-1].strip() if value else None
            value = [
                j.strip()
                for i, j in data
                if i in ["Kích thước, khối lượng", "Khối lượng"]
            ]
            return (
                re.sub(r"(.*Nặng\s+)?(\d+\.?\d*)\s*[g(].*", r"\2", value[0]) + " g"
                if value
                else None
            )

        def material():
            value = [
                j.strip() for i, j in data if i in ["Chất liệu khung viền", "Chất liệu"]
            ]
            return value[0] if value else None

        def connectivity():
            value = {
                j.strip() if j else ""
                for i, j in data
                if i
                in [
                    "Wifi",
                    "Bluetooth",
                    "Kết nối khác",
                    "Kết nối không dây",
                    "Kết nối",
                    "Công nghệ kết nối",
                ]
            }
            return ", ".join(value) if value else None

        def network():
            value = [j.strip() for i, j in data if i == "Mạng di động"]
            return value[0] if value else None

        def ports():
            value = {
                j.strip() if j else ""
                for i, j in data
                if i
                in [
                    "Jack tai nghe",
                    "Cổng kết nối/sạc",
                    "Cổng giao tiếp",
                    "Cổng sạc",
                    "Jack cắm",
                    "Cổng kết nối",
                ]
            }
            return ", ".join(value) if value else None

        def sound_tech():
            value = [j.strip() for i, j in data if i == "Công nghệ âm thanh"]
            return value[0] if value else None

        def speaker_driver():
            value = [j.strip() for i, j in data if i == "Công nghệ âm thanh"]
            return (
                re.sub(r".*(Driver\s+\d+\s*mm).*", r"\1", value[0])
                if value and re.findall(r"Driver\s+\d+\s*mm", value[0])
                else None
            )

        def compatible():
            value = [j.strip() for i, j in data if i == "Tương thích"]
            return value[0] if value else None

        def control():
            value = [j.strip() for i, j in data if i == "Điều khiển"]
            return value[0] if value else None

        def case_battery():
            value = [j.strip() for i, j in data if i == "Thời lượng pin hộp sạc"]
            return value[0] if value else None

        def power_consumption():
            value = [j.strip() for i, j in data if i == "Công suất tiêu thụ điện"]
            return value[0] if value else None

        match device:
            case "phone":
                return {
                    "cpu": cpu(),
                    "cpu_speed": cpu_speed(),
                    "gpu": gpu(),
                    "ram": ram(),
                    "storage": storage(),
                    "rearcam_specs": rearcam_specs(),
                    "frontcam_specs": frontcam_specs(),
                    "screen_type": screen_type(),
                    "screen_size": screen_size(),
                    "screen_panel": screen_panel(),
                    "screen_res": screen_res(),
                    "screen_rate": screen_rate(),
                    "screen_nits": screen_nits(),
                    "os": os(),
                    "water_resistant": water_resistant(),
                    "battery": battery(),
                    "charger": charger(),
                    "weight": weight(),
                    "material": material(),
                    "connectivity": connectivity(),
                    "network": network(),
                    "ports": ports(),
                }
            case "tablet":
                return {
                    "cpu": cpu(),
                    "cpu_speed": cpu_speed(),
                    "gpu": gpu(),
                    "ram": ram(),
                    "storage": storage(),
                    "rearcam_specs": rearcam_specs(),
                    "frontcam_specs": frontcam_specs(),
                    "screen_size": screen_size(),
                    "screen_panel": screen_panel(),
                    "screen_res": screen_res(),
                    "screen_rate": screen_rate(),
                    "os": os(),
                    "water_resistant": water_resistant(),
                    "battery": battery(),
                    "charger": charger(),
                    "weight": weight(),
                    "material": material(),
                    "connectivity": connectivity(),
                    "network": network(),
                    "ports": ports(),
                }
            case "laptop":
                return {
                    "cpu": cpu(),
                    "cpu_cores": cpu_cores(),
                    "cpu_threads": cpu_threads(),
                    "cpu_speed": cpu_speed(),
                    "gpu": gpu(),
                    "ram": ram(),
                    "max_ram": max_ram(),
                    "ram_type": ram_type(),
                    "ram_bus": ram_bus(),
                    "storage": storage(),
                    "webcam": webcam(),
                    "screen_panel": screen_panel(),
                    "screen_size": screen_size(),
                    "screen_tech": screen_tech(),
                    "screen_res": screen_res(),
                    "screen_rate": screen_rate(),
                    "screen_nits": screen_nits(),
                    "os": os(),
                    "battery": battery(),
                    "weight": weight(),
                    "material": material(),
                    "connectivity": connectivity(),
                    "ports": ports(),
                }
            case "watch":
                return {
                    "cpu": cpu(),
                    "storage": storage(),
                    "screen_type": screen_type(),
                    "screen_panel": screen_panel(),
                    "screen_size": screen_size(),
                    "os": os(),
                    "water_resistant": water_resistant(),
                    "connectivity": connectivity(),
                    "battery": battery(),
                    "weight": weight(),
                    "material": material(),
                }
            case "earphone":
                return {
                    "sound_tech": sound_tech(),
                    "speaker_driver": speaker_driver(),
                    "compatible": compatible(),
                    "control": control(),
                    "water_resistant": water_resistant(),
                    "ports": ports(),
                    "connectivity": connectivity(),
                    "battery": battery(),
                    "case_battery": case_battery(),
                    "weight": weight(),
                }
            case "screen":
                return {
                    "screen_type": screen_type(),
                    "screen_panel": screen_panel(),
                    "screen_size": screen_size(),
                    "screen_tech": screen_tech(),
                    "screen_res": screen_res(),
                    "screen_rate": screen_rate(),
                    "power_consumption": power_consumption(),
                    "ports": ports(),
                    "weight": weight(),
                }

    async def __scrape(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ):
        full_data = None  # include some specs info but messy
        specs_data = []  # full specs info
        product = None
        path = None
        include = [
            ("*", "@id='productld'", ""),  # tag, condition, ancestor
            ("span", "@class='circle'", "/ancestor::li"),
            ("a", "contains(@class,'tzLink')", "/ancestor::li"),
            ("span", "@class=''", "/ancestor::li"),
        ]

        fetched = await Crawler.async_inspect(
            url,
            xpath="|".join([f"//{i[0]}[{i[1]}]{i[2]}" for i in include]),
            encoding="utf-8",
            client=client,
            semaphore=semaphore,
        )

        # check fetched data
        if not fetched:
            async with Scraper.__lock:
                Scraper.__scraped.add(url)
                Scraper.__queue.discard(url)
            return

        # prepare data
        data = [
            re.sub(r"\s{2,}", ", ", i.text_content().strip())
            for i in fetched
            if ":" in i.text_content()
        ]  # remove noise from fetched json
        json_content = [i for i in data if re.findall(r"{|}", i)]
        tags_content = [
            (
                i.split(":")[0].strip(),
                "".join(i.split(":")[1:]).removeprefix(",").strip(),
            )
            for i in data
            if not re.findall(r"{|}", i)
        ]

        if not json_content or not tags_content:  # if separating failed
            async with Scraper.__lock:
                Scraper.__scraped.add(url)
                Scraper.__queue.discard(url)
            return

        full_data = json.loads(json_content[0])
        specs_data.extend(tags_content)

        # parse product info
        if url.split("/")[3] == "dtdd":
            product = Phone(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "phone"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_phones_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "laptop":
            product = Laptop(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "laptop"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_laptops_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "may-tinh-bang":
            product = Tablet(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "tablet"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_tablets_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "dong-ho-thong-minh":
            product = Watch(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "watch"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_watches_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "tai-nghe":
            product = Earphone(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "earphone"),
            )
            path = os.path.join(
                Scraper.saving_dir,
                f"{Scraper.__retailer.lower()}_earphones_{datetime.now().strftime("%Y-%m-%d")}.csv",
            )
            if not Scraper.__saving_paths.get(path):
                Scraper.__saving_paths[path] = 0
        elif url.split("/")[3] == "man-hinh-may-tinh":
            product = Screen(
                **Scraper.__parse_common_info(full_data),
                **Scraper.__parse_specs_info(specs_data, "screen"),
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
            Scraper.result.append(asdict(product))

            if Scraper.saving_dir:
                if os.path.exists(path):  # remove duplicate files in a same date
                    if Scraper.__saving_paths[path] == 0 and os.path.getsize(path):
                        os.remove(path)
                        Scraper.__saving_paths.update({path: 1})  # update file status
                else:
                    Scraper.__saving_paths.update({path: 1})

                dict_to_csv(asdict(product), path)

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
