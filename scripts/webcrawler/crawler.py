from utils import async_get, save_to_file, colorized
from lxml import html
from urllib.parse import urljoin
from datetime import datetime
import asyncio, httpx, os


class Crawler:
    """
    Asynchronous URL crawler that discovers and processes links based on an XPath search expression.

    Attributes
    ----------
    base_url: str
        The starting URL for the crawling process.
    search: str
        XPath expression used to locate target links in HTML pages.
    save_in: str
        Path to save output data.
    """

    base_url = None
    search = None
    save_path = None
    queue = set()
    crawled = set()
    valid = set()
    history = set()
    __lock = asyncio.Lock()

    def __init__(self, base_url: str, *, search: str, save_in: str = None):
        if not Crawler.base_url:
            Crawler.base_url = base_url
            Crawler.queue.add(base_url)

        if not Crawler.search:
            Crawler.search = search

        if save_in:
            Crawler.save_path = save_in
            Crawler.__history_check()

    @staticmethod
    async def async_inspect(
        url,
        *,
        client: httpx.AsyncClient,
        xpath: str = None,
        encoding: str = "utf-8",
        semaphore: asyncio.Semaphore = None,
    ):
        """
        Asynchronously inspect HTML content from given URL.
        """

        resp = await async_get(url, client=client, semaphore=semaphore)

        if not resp:
            print(f"Inspecting {url} failed.")
            return

        source = html.fromstring(resp.content.decode(encoding))

        if xpath:
            return source.xpath(xpath)

        return html.tostring(source, pretty_print=True, encoding="unicode")

    async def __crawl(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore = None,
    ):
        found = await Crawler.async_inspect(
            url, client=client, xpath=Crawler.search, semaphore=semaphore
        )

        if not found:  # url broken
            async with Crawler.__lock:
                Crawler.queue.remove(url)
            return

        result = [str(urljoin(Crawler.base_url, i)).strip() for i in found]

        # update results
        async with Crawler.__lock:
            Crawler.queue.remove(url)  # remove inspected url
            Crawler.queue.update(
                i for i in result if i not in Crawler.crawled
            )  # put new urls into queue for inspecting
            Crawler.crawled.add(url)  # put inspected url into crawled for history check
            Crawler.crawled.update(result)  # also put founded urls into crawled
            Crawler.valid.add(url)  # put only scrapable urls into valid

            if Crawler.save_path and url not in Crawler.history:
                save_to_file(
                    f"{url}, {datetime.now()}" + "\n", Crawler.save_path
                )  # only save new valid urls

    @classmethod
    async def execute(
        cls,
        *,
        timeout: int | float = 10.0,
        follow_redirects: bool = True,
        headers: httpx.Headers = None,
        chunksize: int = 100,
        semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    ):
        """
        Start crawling process from given base URL.

        Parameters
        ---
        timeout: int, float, optional
            Request timeout in seconds (default: **10.0**).
        follow_redirects: bool, optional
            Whether to follow HTTP redirects (default: **True**).
        headers: httpx.Header, optional
            Custom HTTP request headers (default: **None**).
        chunksize: int, optional
            Number of URLs to process per batch, best range in **100-2000** (default: **100**).
        semaphore: asyncio.Semaphore, optional
            Concurrency limit for simultaneous requests, best range in **10-200** (default: **10**).
        """

        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
            "Connection": "keep-alive",
            "Accept-Language": "vi, en, en-US;q=0.9, en-GB;q=0.9",
            "Cache-Control": "no-cache",
            "Referer": "https://www.google.com/",
            "DNT": "1",  # not track request header
            "Upgrade-Insecure-Requests": "1",
        }

        if not headers:
            headers = default_headers
        else:
            copy = default_headers.copy()
            copy.update(headers)
            headers = copy

        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=follow_redirects,
            headers=headers,
        ) as client:
            while cls.queue:
                in_use = list(cls.queue)[:chunksize]
                tasks = [
                    asyncio.create_task(cls.__crawl(i, client, semaphore))
                    for i in in_use
                ]

                await asyncio.gather(*tasks)

                print(
                    f"Pending {len(cls.queue)} | Crawled: {len(cls.crawled)} | Valid: {len(cls.valid)}"
                )

        if cls.history:
            new = len(cls.valid - cls.history)
            text = (
                f"(Found {new} more {'urls'if new>1 else 'url'})"
                if new > 0
                else "(No more urls found)"
            )

        print(
            f"Crawled: {colorized(len(cls.crawled),33)} | Valid: {colorized(len(cls.valid),32)} {text if cls.history else ''}"
        )

    def __history_check():
        if not os.path.isfile(Crawler.save_path):
            return

        print("Previous work detected. Continuing...")

        try:
            with open(Crawler.save_path, "r") as file:
                for i in file:
                    Crawler.history.add(str(i).removesuffix("\n"))
                    Crawler.valid.add(str(i).removesuffix("\n"))
            print("History updated.")
        except Exception as e:
            print(f"Error occurs while opening file >> {e}")

    @classmethod
    def reset(cls):
        """
        Reset crawler after use.
        """

        if cls.save_path:
            cls.save_path = None

        cls.base_url = None
        cls.search = None
        cls.queue.clear()
        cls.crawled.clear()
        cls.history.clear()

        print("Crawler reset.")
