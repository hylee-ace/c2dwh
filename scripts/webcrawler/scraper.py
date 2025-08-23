import httpx, json, asyncio, os
from .crawler import Crawler
from .models import Product
from utils import colorized, dict_to_csv
from py_mini_racer.py_mini_racer import MiniRacer
from datetime import datetime


class CpsScraper:
    """
    Asynchronous URL scraper that fetches desired information from HTML contents.

    Attributes
    ----------
    urls: list[str]
        The list of URLs waiting for being scraped.
    save_in: str
        Directory for saved output.
    """

    __retailer = None
    saving_dir = None
    __queue = set()
    __scraped = set()
    result = list()
    __is_removed = False
    __lock = asyncio.Lock()

    def __init__(self, urls: list[str], *, save_in: str = None):
        CpsScraper.__queue.update(urls)
        CpsScraper.saving_dir = save_in

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
        data = await CpsScraper.nuxt_to_data(
            url, client=client, semaphore=semaphore, encoding="utf-8"
        )
        islaptop = False

        # check if url were product page
        if not data or "product-detail:0" not in data["fetch"]:
            async with CpsScraper.__lock:
                CpsScraper.__scraped.add(url)
                CpsScraper.__queue.discard(url)
            return

        # check device type
        for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
            "additionalProperty"
        ]:
            if i["name"] == "Loại card đồ họa":
                islaptop = True
                break

        product = Product(
            data["data"][0]["pageInfo"]["product_id"],
            data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                "name"
            ],
            url=url,
        )

        # parse general info
        if data["fetch"]["product-detail:0"]["variants"]:
            product.price = int(
                sum(
                    [
                        i["filterable"]["price"]
                        for i in data["fetch"]["product-detail:0"]["variants"]
                    ]
                )
                / len(data["fetch"]["product-detail:0"]["variants"])
            )
            product.onsale_price = int(
                sum(
                    [
                        i["filterable"]["special_price"]
                        for i in data["fetch"]["product-detail:0"]["variants"]
                    ]
                )
                / len(data["fetch"]["product-detail:0"]["variants"])
            )
            product.stock = sum(
                i["filterable"]["stock"]
                for i in data["fetch"]["product-detail:0"]["variants"]
            )

        if data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"].get(
            "aggregateRating"
        ):
            product.rating = float(
                data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                    "aggregateRating"
                ]["ratingValue"],
            )
            product.reviews_count = int(
                data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                    "aggregateRating"
                ]["reviewCount"],
            )

        product.brand = data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
            "json"
        ]["brand"]["name"]
        product.is_new = (
            False
            if "cũ"
            in data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                "name"
            ]
            .lower()
            .split(" ")
            else True
        )
        CpsScraper.__retailer = product.retailer = [
            i.get("content")
            for i in data["data"][0]["head"]["meta"]
            if i.get("property") == "og:site_name"
        ][0]
        product.created_at = datetime.fromisoformat(
            data["data"][0]["pageInfo"]["created_at"]
        ).strftime("%Y-%m-%d %H:%M:%S")
        os_value = [
            i["value"]
            for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                "json"
            ]["additionalProperty"]
            if i["name"] == "Hệ điều hành"
        ]

        # classify categories
        if islaptop:
            product.category = "Laptop"
            product.os = os_value[0] if os_value else None
            product.cpu = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Loại CPU"
            ][0]
            product.gpu = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Loại card đồ họa"
            ][0]
        else:
            ram_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Dung lượng RAM"
            ]
            product.category = "Smartphone" if ram_value else "Phone"
            product.os = os_value[0] if os_value else None

        # update results
        async with CpsScraper.__lock:
            CpsScraper.__scraped.add(url)
            CpsScraper.__queue.discard(url)
            CpsScraper.result.append(product.info())

            if CpsScraper.saving_dir:
                path = os.path.join(
                    CpsScraper.saving_dir,
                    product.retailer.lower()
                    + "_products_"
                    + f"{datetime.now().strftime("%Y-%m-%d")}.csv",
                )

                if os.path.exists(path):  # remove duplicate files in a day
                    if not CpsScraper.__is_removed and os.path.getsize(path):
                        os.remove(path)
                        CpsScraper.__is_removed = True

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
            Delay between chunks (default: **2.0**).
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

        print(f"Scraper for {CpsScraper.__retailer} reset.")

        if cls.saving_dir:
            cls.saving_dir = None

        cls.__retailer = None
        cls.__queue.clear()
        cls.__scraped.clear()
        cls.result.clear()
