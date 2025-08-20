import httpx, json, asyncio
from .crawler import Crawler
from .models import Phone, Laptop
from utils import colorized, dict_to_csv
from py_mini_racer.py_mini_racer import MiniRacer
from datetime import datetime


class CpsScraper:
    __retailer = "CellphoneS"
    save_path = None
    result = list()
    __queue = set()
    __scraped = set()
    __lock = asyncio.Lock()

    def __init__(self, urls: list[str], *, save_in: str = None):
        CpsScraper.__queue.update(urls)
        CpsScraper.save_path = save_in

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

        # parse general info
        def parse_general_info(product: Laptop | Phone):
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
            ]["json"]["brand"]["name"]
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
            product.created_at = datetime.fromisoformat(
                data["data"][0]["pageInfo"]["created_at"]
            ).strftime("%Y-%m-%d %H:%M:%S")

        # parse info
        if islaptop:
            laptop = Laptop(
                data["data"][0]["pageInfo"]["product_id"],
                data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                    "name"
                ],
                url=url,
                retailer=CpsScraper.__retailer,
            )

            parse_general_info(laptop)

            # specific info
            laptop.cpu = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Loại CPU"
            ][0]
            laptop.gpu = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Loại card đồ họa"
            ][0]

            # update result
            async with CpsScraper.__lock:
                CpsScraper.__scraped.add(url)
                CpsScraper.__queue.discard(url)
                CpsScraper.result.append(laptop.info())
                if CpsScraper.save_path:
                    dict_to_csv(laptop.info(), CpsScraper.save_path)

        else:
            phone = Phone(
                data["data"][0]["pageInfo"]["product_id"],
                data["fetch"]["product-detail:0"]["headProduct"]["script"][1]["json"][
                    "name"
                ],
                url=url,
                retailer=CpsScraper.__retailer,
            )

            parse_general_info(phone)

            # specific info
            os_value = [
                i["value"]
                for i in data["fetch"]["product-detail:0"]["headProduct"]["script"][1][
                    "json"
                ]["additionalProperty"]
                if i["name"] == "Hệ điều hành"
            ]

            if os_value:
                phone.os = os_value[0]
            else:
                phone.category = "Phone"

            # update result
            async with CpsScraper.__lock:
                CpsScraper.__scraped.add(url)
                CpsScraper.__queue.discard(url)
                CpsScraper.result.append(phone.info())
                if CpsScraper.save_path:
                    dict_to_csv(phone.info(), CpsScraper.save_path)

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

    def __history_check():
        pass

    @classmethod
    def reset(cls):
        """
        Reset scraper after use.
        """

        print(f"Scraper for {CpsScraper.__retailer} reset.")

        if cls.save_path:
            cls.save_path = None

        cls.__queue.clear()
        cls.result.clear()
