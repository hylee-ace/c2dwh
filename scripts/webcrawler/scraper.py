import httpx, json, asyncio, os
from .crawler import Crawler
from .models import Product
from utils import colorized, dict_to_csv
from py_mini_racer import MiniRacer
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
    saving_path = None
    __queue = set()
    __scraped = set()
    result = list()
    __is_removed = False
    __lock = asyncio.Lock()

    def __init__(self, urls: list[str], *, target: str, save_in: str = None):
        Scraper.__queue.update(urls)
        Scraper.__retailer = target.upper()

        if save_in:
            Scraper.saving_path = os.path.join(
                save_in,
                Scraper.__retailer.lower()
                + "_products_"
                + f"{datetime.now().strftime("%Y-%m-%d")}.csv",
            )

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

    async def __parse_data(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ):
        data = None
        product = None
        islaptop = False

        if Scraper.__retailer == "CELLPHONES":
            data = await Scraper.nuxt_to_data(
                url, client=client, semaphore=semaphore, encoding="utf-8"
            )

            # check if url were product page
            if not data or "product-detail:0" not in data["fetch"]:
                async with Scraper.__lock:
                    Scraper.__scraped.add(url)
                    Scraper.__queue.discard(url)
                return

            # check device type
            for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                "json"
            ]["additionalProperty"]:
                if i["name"] == "Loại card đồ họa":
                    islaptop = True
                    break

            product = Product(
                product_id=data["data"][0]["pageInfo"]["product_id"],
                name=data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["name"].strip(),
                url=url,
                retailer=Scraper.__retailer,
            )

            # parse general info
            if data["fetch"]["product-detail:0"]["variants"]:
                product.onsale_price = int(
                    sum(
                        [
                            (
                                i["filterable"]["special_price"]
                                if i["filterable"]["special_price"] != 0
                                else i["filterable"]["price"]
                            )
                            for i in data["fetch"]["product-detail:0"]["variants"]
                        ]
                    )
                    / len(data["fetch"]["product-detail:0"]["variants"])
                )

            if data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                "json"
            ].get("aggregateRating"):
                product.rating = float(
                    data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                        "json"
                    ]["aggregateRating"]["ratingValue"],
                )
                product.reviews_count = int(
                    data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                        "json"
                    ]["aggregateRating"]["reviewCount"],
                )

            product.brand = data["fetch"]["product-detail:0"]["headProduct"]["script"][
                1
            ]["json"]["brand"]["name"].strip()
            product.is_new = (
                False
                if "cũ"
                in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["name"]
                .lower()
                .split(" ")
                else True
            )
            product.release_date = datetime.fromisoformat(
                data["data"][0]["pageInfo"]["created_at"]
            ).strftime("%Y-%m-%d %H:%M:%S")

            os_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Hệ điều hành"
            ]
            ram_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Dung lượng RAM" or i["name"] == "Loại RAM"
            ]
            storage_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Bộ nhớ trong" or i["name"] == "Ổ cứng"
            ]
            cpu_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Loại CPU" or i["name"] == "Chipset"
            ]

            product.os = os_value[0].strip() if os_value else None
            product.cpu = cpu_value[0].strip() if cpu_value else None
            product.ram = " ".join(ram_value).strip() if ram_value else None
            product.storage = storage_value[0].strip() if storage_value else None

            # classify categories
            if islaptop:
                product.category = "Laptop"
                product.gpu = [
                    i["value"]
                    for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][
                        1
                    ]["json"]["additionalProperty"]
                    if i["name"] == "Loại card đồ họa"
                ][0].strip()
            else:
                product.category = "Smartphone" if ram_value else "Phone"

        else:
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

            # check device type
            for i in data["additionalProperty"]:
                if i["name"] == "Card màn hình":
                    islaptop = True
                    break

            # parse info
            product = Product(
                product_id=data["sku"].strip(),
                name=data["name"].strip(),
                onsale_price=int(data["offers"]["price"]),
                brand=data["brand"]["name"][0].strip(),
                url=data["url"].strip(),
                retailer=Scraper.__retailer,
            )

            if data["aggregateRating"]:
                product.rating = data["aggregateRating"]["ratingValue"]
                product.reviews_count = int(data["aggregateRating"]["reviewcount"])

            released_value = [
                i["value"]
                for i in data["additionalProperty"]
                if i["name"] == "Thời điểm ra mắt"
            ]
            os_value = [
                i["value"]
                for i in data["additionalProperty"]
                if i["name"] == "Hệ điều hành"
            ]
            ram_value = [
                i["value"] if i["value"] != "Hãng không công bố" else ""
                for i in data["additionalProperty"]
                if i["name"] == "RAM"
                or i["name"] == "Loại RAM"
                or i["name"] == "Tốc độ Bus RAM"
            ]
            cpu_value = [
                (
                    "".join(i["value"].split("</a>"))[
                        "".join(i["value"].split("</a>")).rfind(">") + 1 :
                    ]
                    if i["name"] == "Công nghệ CPU"
                    else i["value"]
                )
                for i in data["additionalProperty"]
                if i["name"] == "Chip xử lý (CPU)" or i["name"] == "Công nghệ CPU"
            ]
            storage_value = [
                i["value"]
                for i in data["additionalProperty"]
                if i["name"] == "Dung lượng lưu trữ" or i["name"] == "Ổ cứng"
            ]

            product.release_date = released_value[0].strip() if released_value else None
            product.os = os_value[0].strip() if os_value else None
            product.ram = " ".join(ram_value).strip() if ram_value else None
            product.cpu = cpu_value[0].strip() if cpu_value else None
            product.storage = storage_value[0].strip() if storage_value else None

            # classify categories
            if islaptop:
                gpu_value = "".join(
                    [
                        i["value"]
                        for i in data["additionalProperty"]
                        if i["name"] == "Card màn hình"
                    ][0].split("</a>")
                )
                product.category = "Laptop"
                product.gpu = gpu_value[gpu_value.rfind(">") + 1 :].strip()
            else:
                contact_range = [
                    i["value"]
                    for i in data["additionalProperty"]
                    if i["name"] == "Danh bạ"
                ][0]
                product.category = (
                    "Smartphone" if contact_range == "Không giới hạn" else "Phone"
                )

        # update results
        async with Scraper.__lock:
            Scraper.__scraped.add(url)
            Scraper.__queue.discard(url)
            Scraper.result.append(product.info())

            if Scraper.saving_path:
                # remove duplicate files in a same date
                if os.path.exists(Scraper.saving_path):
                    if not Scraper.__is_removed and os.path.getsize(
                        Scraper.saving_path
                    ):
                        os.remove(Scraper.saving_path)
                        Scraper.__is_removed = True
                else:
                    Scraper.__is_removed = True

                dict_to_csv(product.info(), Scraper.saving_path)

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

        if cls.__retailer not in ["CELLPHONES", "TGDD", "FPTSHOP"]:
            print("Currently only support cellphones, fptshop and tgdd.")
            return

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
                    asyncio.create_task(cls.__parse_data(i, client, semaphore))
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

    @classmethod
    def reset(cls):
        """
        Reset scraper after use.
        """

        print(f"Scraper for {Scraper.__retailer} reset.")

        if cls.saving_path:
            cls.saving_path = None

        cls.__retailer = None
        cls.__queue.clear()
        cls.__scraped.clear()
        cls.result.clear()
