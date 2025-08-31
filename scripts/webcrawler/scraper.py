import httpx, json, asyncio, os
from .crawler import Crawler
from .models import Product, Smartwatch, Smartphone, Laptop, Screen, Tablet, Earphone
from utils import colorized, dict_to_csv
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
    saving_path = None
    __queue = set()
    __scraped = set()
    result = list()
    __is_removed = False
    __lock = asyncio.Lock()

    def __init__(self, urls: list[str], *, save_in: str = None):
        Scraper.__queue.update(urls)

        retailer = "".join(
            [
                i
                for i in urlparse(urls[0]).hostname.split(".")
                if i not in ["com", "vn", "www"]
            ]
        )
        Scraper.__retailer = retailer.upper()

        if save_in:
            Scraper.saving_path = os.path.join(
                save_in,
                f"{retailer}_products_{datetime.now().strftime("%Y-%m-%d")}.csv",
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

        # general info parser
        def parse_common_info(data: dict):
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
            weight_value = [
                i["value"]
                .split(" - ")[-1]
                .removesuffix("g")
                .removeprefix("Nặng")
                .strip()
                for i in data["additionalProperty"]
                if i["name"] == "Kích thước, khối lượng" or i["name"] == "Khối lượng"
            ]
            jack_value = [
                i["value"].strip()
                for i in data["additionalProperty"]
                if i["name"] == "Jack cắm"
            ]

            prd.release_date = released_value[0] if released_value else None

            # check device type
            if prd.url.split("/")[3] == "dtdd":
                prd.category = (
                    "Smartphone"
                    if weight_value and float(weight_value[0]) > 135.0
                    else "Phone"
                )
            elif prd.url.split("/")[3] == "laptop":
                prd.category = "Laptop"
            elif prd.url.split("/")[3] == "dong-ho-thong-minh":
                prd.category = (
                    "Smartwatch"
                    if weight_value and float(weight_value[0]) > 30.0
                    else "Smartband"
                )
            elif prd.url.split("/")[3] == "may-tinh-bang":
                prd.category = "Tablet"
            elif prd.url.split("/")[3] == "tai-nghe":
                if weight_value and float(weight_value[0]) > 190.0:
                    prd.category = "Headphone"
                elif jack_value:
                    prd.category = "Earphone"
                elif not jack_value:
                    prd.category = "Earbuds"
            else:
                prd.category = "Screen"

            return {
                "product_id": prd.product_id,
                "name": prd.name,
                "price": prd.price,
                "brand": prd.brand,
                "category": prd.category,
                "url": prd.url,
                "rating": prd.rating,
                "reviews_count": prd.reviews_count,
                "release_date": prd.release_date,
            }

        # parse general info
        if url.split("/")[3] == "dtdd":
            product = Smartphone(**parse_common_info(data))
        elif url.split("/")[3] == "latop":
            product = Laptop(**parse_common_info(data))
        elif url.split("/")[3] == "may-tinh-bang":
            product = Tablet(**parse_common_info(data))
        elif url.split("/")[3] == "dong-ho-thong-minh":
            product = Smartwatch(**parse_common_info(data))
        elif url.split("/")[3] == "tai-nghe":
            product = Earphone(**parse_common_info(data))
        else:
            product = Screen(**parse_common_info(data))

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
