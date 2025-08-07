from webexplorer import Crawler
from utils import runtime, Cursor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, httpx, asyncio
from lxml import html


@runtime
def main():
    main_page = "https://scrapeme.live/shop/"
    crawled = set()
    lock = threading.Lock()

    # initial step
    Crawler(main_page, crawled)

    # start the work
    with httpx.Client(
        timeout=20.0,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
            "Connection": "keep-alive",
        },
    ) as client:
        while Crawler.queue:
            # limit thread workers
            workers = list(Crawler.queue)[:100]

            with ThreadPoolExecutor(max_workers=100) as ex:
                futures = [
                    ex.submit(
                        Crawler.crawl,
                        url=i,
                        lock=lock,
                        output=crawled,
                        client=client,
                    )
                    for i in workers
                ]

                for _ in as_completed(futures):
                    print(
                        f"Checklist remains {len(Crawler.queue)} | Passed {len(Crawler.crawled)}"
                    )

    print(f"Found {len(crawled)} urls.", Cursor.clear)
    print(Cursor.reveal)


if __name__ == "__main__":
    main()
