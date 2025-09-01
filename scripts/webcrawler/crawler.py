import asyncio, httpx, os
from utils import colorized, dict_to_csv, s3_file_uploader
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
    save_in: str, optional
        Directory for saved output, the output file could be used for crawling-work history check in future (default: **None**).
    upload_to_s3: bool, optional
        For uploading result file to AWS S3 bucket (default: **False**).
    s3_attrs: dict, optional
        Provide S3 attributes such as **client** (optional), **bucket** and **obj_prefix** for uploading (default: **None**).
    """

    base_url = None
    search = None
    saving_path = None
    __queue = set()
    __crawled = set()
    result = set()
    __history = set()
    upload_to_s3 = False
    s3_attrs = dict()
    __lock = asyncio.Lock()

    def __init__(
        self,
        base_url: str,
        *,
        search: str,
        save_in: str | None = None,
        upload_to_s3: bool = False,
        s3_attrs: dict | None = None,
    ):
        Crawler.base_url = base_url
        Crawler.search = search
        Crawler.__queue.add(base_url)

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
                + "_urls.csv",
            )
            Crawler.__history_check()

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

            Crawler.upload_to_s3 = upload_to_s3
            Crawler.s3_attrs = s3_attrs

    @staticmethod
    async def async_inspect(
        url,
        *,
        client: httpx.AsyncClient | None = None,
        xpath: str | None = None,
        semaphore: asyncio.Semaphore | None = None,
        encoding: str | None = None,
        retries: int = 3,
        retry_delay: float = 2.0,
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
                    print(f"{repr(e)}. Retry after {retry_delay}sec...")
                    last_exception = e
                    await asyncio.sleep(retry_delay)

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
        headers: httpx.Headers | None = None,
        chunksize: int = 20,
        semaphore: asyncio.Semaphore | None = None,
        delay: float | None = None,
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

        # upload to s3 bucket
        if cls.upload_to_s3 and cls.s3_attrs:
            if len(cls.result - cls.__history) > 0:
                bucket = cls.s3_attrs["bucket"]
                filename = os.path.basename(cls.saving_path)
                key = f"{cls.s3_attrs['obj_prefix'] if cls.s3_attrs.get('obj_prefix') else ''}{filename}"

                print(f"Start uploading {filename} to {bucket}...")
                s3_file_uploader(
                    cls.saving_path,
                    client=cls.s3_attrs.get("client"),
                    bucket=bucket,
                    key=key,
                )
                print(f"Uploading {filename} successfully.")
            else:
                print("Uploading cancelled since no more urls found.")

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

        if cls.saving_path:
            cls.saving_path = None

        cls.base_url = None
        cls.search = None
        cls.result.clear()
        cls.__queue.clear()
        cls.__crawled.clear()
        cls.__history.clear()

        print(f"Crawler for {urlparse(cls.base_url).hostname} reset.")
