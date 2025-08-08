from utils import async_get
from lxml import html
from urllib.parse import urljoin
import asyncio, httpx


class Crawler:
    base_url = None
    search = None
    save_in = set()
    queue = set()
    crawled = set()
    lock = asyncio.Lock()

    def __init__(self, base_url: str, *, search: str, save_in: set = None):
        if not Crawler.base_url:
            Crawler.base_url = base_url
            Crawler.queue.add(base_url)

        if not Crawler.search:
            Crawler.search = search

        Crawler.save_in = save_in

    @staticmethod
    async def inspect(
        url,
        *,
        client: httpx.AsyncClient,
        xpath: str = None,
        semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    ):
        resp = await async_get(url, client=client, semaphore=semaphore)

        if not resp:
            print(f"Inspecting {url} failed.")
            return

        source = html.fromstring(resp.content)

        if xpath:
            return source.xpath(xpath)

        return html.tostring(source, pretty_print=True, encoding="unicode")

    async def __crawl(
        url: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ):
        found = await Crawler.inspect(
            url, client=client, xpath=Crawler.search, semaphore=semaphore
        )

        if not found:  # url broken
            Crawler.queue.remove(url)
            return

        result = [urljoin(Crawler.base_url, i) for i in found]

        # update results
        async with Crawler.lock:
            Crawler.queue.remove(url)  # remove inspected url
            Crawler.queue.update(
                i for i in result if i not in Crawler.crawled
            )  # put new urls in queue for inspecting
            Crawler.crawled.add(url)  # put inspected url to crawled
            Crawler.crawled.update(result)  # also put founded urls to

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
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
                "Connection": "keep-alive",
            }
        while cls.queue:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=follow_redirects,
                headers=headers,
            ) as client:
                in_use = list(cls.queue)[:chunksize]
                tasks = [
                    asyncio.create_task(cls.__crawl(i, client, semaphore))
                    for i in in_use
                ]

                await asyncio.gather(*tasks)

            print(
                f"Checklist remains {len(cls.queue)} | Found: {len(cls.crawled)} urls"
            )

    @classmethod
    def reset(cls):
        if cls.save_in:
            cls.save_in.clear()

        cls.base_url = None
        cls.search = None
        cls.queue.clear()
        cls.crawled.clear()
