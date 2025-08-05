from web_crawler.utils import runtime, colorized, Cursor, CustomDriver
from web_crawler.crawler import Crawler
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
import threading


@runtime
def crawl(url: str):
    crawler = Crawler()
    crawler.queue.add(url)
    lock = threading.Lock()

    while crawler.queue:
        temp = set()

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [
                ex.submit(
                    Crawler.inspect,
                    i,
                    helper="bs4",
                    tag="a",
                    attr="href",
                    href=lambda x: x
                    and urlparse(urljoin(url, i)).hostname == urlparse(url).hostname
                    and urlparse(urljoin(url, i)).path.endswith("html")
                    and len(x) > 1
                    and x != "",
                )
                for i in crawler.queue
            ]

            for i in as_completed(futures):
                link = urlparse(urljoin(url, i.result()))
                try:
                    with lock:  # prevent race condition
                        crawler.crawled.update(crawler.queue, link)
                        temp.update(link)
                except Exception as e:
                    print(f"[{colorized('x',31,bold=True)}] Error >> {e}")
                    continue

        crawler.history.update(crawler.queue)
        crawler.queue = temp.difference(crawler.queue)
        crawler.queue.difference_update(crawler.history)

    print(
        f"Exploring completed. Found {colorized(str(len(crawler.crawled))+' urls',32)}.",
        Cursor.clear,
    )


def main():
    domain = "https://cellphones.com.vn/"


if __name__ == "__main__":
    main()
