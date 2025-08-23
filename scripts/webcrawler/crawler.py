import asyncio, httpx, os
from utils import colorized, dict_to_csv
from lxml import html
from urllib.parse import urljoin, urlparse
from datetime import datetime


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
        Directory for saved output.
    """

    base_url = None
    search = None
    saving_path = None
    __queue = set()
    __crawled = set()
    result = set()
    __history = set()
    __lock = asyncio.Lock()

    def __init__(self, base_url: str, *, search: str, save_in: str = None):
        if not Crawler.base_url:
            Crawler.base_url = base_url
            Crawler.__queue.add(base_url)

        if not Crawler.search:
            Crawler.search = search

        if save_in:
            Crawler.saving_path = os.path.join(
                save_in,
                "".join(
                    [
                        i
                        for i in urlparse(base_url).hostname.split(".")
                        if i not in ["com", "vn", "www"]
                    ]
                )
                + ".csv",
            )
            Crawler.__history_check()

    @staticmethod
    async def async_inspect(
        url,
        *,
        client: httpx.AsyncClient = None,
        xpath: str = None,
        semaphore: asyncio.Semaphore = None,
        encoding: str = None,
        retries: int = 3,
        delay: float = 2.0,
    ):
        """
        Asynchronously inspect HTML content from given URL.
        """
        last_exception = None
        resp = None

        if not semaphore:  # limit number of concurrent processes
            semaphore = asyncio.Semaphore(5)

        async def get_response(
            client: httpx.AsyncClient = None,
        ):  # handle retries
            nonlocal resp, last_exception
            for _ in range(retries):
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    break
                except httpx.HTTPStatusError as e:
                    print(f"Inspecting {url} failed >> {e}")
                    return
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    print(f"{repr(e)}. Retrying...")
                    last_exception = e
                    await asyncio.sleep(delay)

        async with semaphore:
            if client:  # global client
                await get_response(client)
            else:
                async with httpx.AsyncClient(
                    timeout=10.0,
                    headers={
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0",
                        "Connection": "keep-alive",
                    },
                ) as client:
                    await get_response(client)

        if not resp:  # in case failed 3 times
            print(f"Inspecting {url} failed 3 times >> {last_exception}")
            return

        source = html.fromstring(
            resp.content if not encoding else resp.content.decode(encoding)
        )

        if xpath:
            return source.xpath(xpath)

        return html.tostring(source, pretty_print=True, encoding="unicode")

    async def __crawl(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ):
        found = await Crawler.async_inspect(
            url, client=client, xpath=Crawler.search, semaphore=semaphore
        )

        if not found:  # url might be broken or facing IP banned
            async with Crawler.__lock:
                Crawler.__queue.remove(url)
            return

        result = [str(urljoin(Crawler.base_url, i)).strip() for i in found]

        # update results
        async with Crawler.__lock:
            Crawler.__queue.remove(url)  # remove inspected url
            Crawler.__queue.update(
                i for i in result if i not in Crawler.__crawled
            )  # put new urls into queue for inspecting

            if url not in Crawler.__history:
                Crawler.__crawled.add(url)  # put inspected url into crawled
                Crawler.result.add(url)  # only save valid urls
                if Crawler.saving_path:
                    dict_to_csv(
                        {
                            "url": url,
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        Crawler.saving_path,
                    )  # only save new valid urls

            Crawler.__crawled.update(result)  # also put found urls into crawled

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
                    asyncio.create_task(cls.__crawl(i, client, semaphore))
                    for i in in_use
                ]

                await asyncio.gather(*tasks)

                if delay:
                    await asyncio.sleep(delay)  # delay between chunks

                print(
                    f"From: {cls.base_url}",
                    f"Pending {len(cls.__queue)}",
                    f"Crawled: {len(cls.__crawled)}",
                    f"Valid: {len(cls.result)}",
                    sep=" | ",
                )

        if cls.__history:
            new = len(cls.result - cls.__history)
            text = (
                f"(Found {new} more {'urls'if new>1 else 'url'})"
                if new > 0
                else "(No more urls found)"
            )

        print("Crawling successfully.")
        print(
            f"From: {colorized( cls.base_url,34)}",
            f"Crawled: {colorized(len(cls.__crawled),33)}",
            f"Valid: {colorized(len(cls.result),32)} {text if cls.__history else ''}",
            sep=" | ",
        )

    def __history_check():
        if not os.path.isfile(Crawler.saving_path):
            return

        print(f"Previous work with {Crawler.base_url} detected. Continuing...")

        try:
            with open(Crawler.saving_path, "r") as file:
                next(file)
                for i in file:
                    url = str(i).split(",")[0]
                    Crawler.__history.add(url)
                    Crawler.result.add(url)
                    Crawler.__queue.add(url)
                    Crawler.__crawled.add(url)
            print("History updated.")
        except Exception as e:
            print(f"Error occurs while opening file >> {e}")

    @classmethod
    def reset(cls):
        """
        Reset crawler after use.
        """

        print(f"Crawler for {urlparse(cls.base_url).hostname} reset.")

        if cls.saving_path:
            cls.saving_path = None

        cls.base_url = None
        cls.search = None
        cls.result.clear()
        cls.__queue.clear()
        cls.__crawled.clear()
        cls.__history.clear()
