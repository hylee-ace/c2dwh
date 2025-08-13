from webcrawler import Crawler
from utils import runtime
import asyncio, multiprocessing


def crawl_site(
    page: str,
    xpath: str,
    path: str,
    *,
    timeout: float | int,
    chunksize: int,
    semaphore: int,
):
    crawler = Crawler(page, search=xpath, save_in=path)

    asyncio.run(
        crawler.execute(
            timeout=timeout, chunksize=chunksize, semaphore=asyncio.Semaphore(semaphore)
        )
    )


@runtime
def main():
    sites = [
        {
            "page": "https://cellphones.com.vn/",
            "xpath": "//a[substring(@href,string-length(@href)-4)='.html' and contains(@href,'https://cellphones.com.vn/')]/@href",
            "path": "./scripts/webcrawler/crawled/cellphones.csv",
            "timeout": 20.0,
            "chunksize": 1000,
            "semaphore": 200,
        },
        {
            "page": "https://www.thegioididong.com/",
            "xpath": "//a[not(ancestor::div[@class='header__top' or @class='header-top-bar'"
            + "or @class='box-listing-news'] or ancestor::footer[@class='footer v2024']or contains(@href,';')"
            + "or contains(@href,'#')or contains(@href,'&')or contains(@href,'?')or contains(@href,'tien-ich')"
            + "or contains(@href,'tin-tuc'))]/@href",
            "path": "./scripts/webcrawler/crawled/tgdd.csv",
            "timeout": 20.0,
            "chunksize": 20,
            "semaphore": 5,
        },
    ]
    processes: list[multiprocessing.Process] = []

    for i in sites:
        p = multiprocessing.Process(target=crawl_site, kwargs=i)
        p.start()
        processes.append(p)

    for i in processes:
        i.join()


if __name__ == "__main__":
    main()
