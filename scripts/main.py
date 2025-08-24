import asyncio, multiprocessing, random, time
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader


def crawling_work(
    site: str,
    xpath: str,
    chunksize: int,
    sema: int,
    headers=None,
):
    crawler = Crawler(site, search=xpath, save_in="./scripts/webcrawler/crawled")

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            headers=headers,
            delay=random.uniform(0.5, 1.0),
        )
    )

    crawler.reset()


def crawling_process():
    excludes = [
        "dmca",
        "do-gia-dung",
        "nha-thong-minh",
        "man-hinh",
        "hang-cu",
        "do-choi",
        "may-anh",
        "flycam",
        "may-in",
        "tablet",
        "sim",
        "linh-kien",
        "may-tinh",
        "am-thanh",
        "phu-kien",
        "tivi",
        "pin",
        "op-lung",
        "sac",
        "chan-de",
        "cap",
        "imac",
    ]
    exclude_text = "or ".join(f"contains(@href,'{i}')" for i in excludes)

    works = [
        {
            "site": "https://cellphones.com.vn/",
            "xpath": f"//a[substring(@href,string-length(@href)-4)='.html'and not({exclude_text})"
            "and contains(@href,'cellphones.com.vn')]/@href",
            "chunksize": 50,
            "sema": 50,
        },
        {
            "site": "https://www.thegioididong.com/",
            "xpath": "//a[substring(@href,1,7)='/laptop'or substring(@href,1,5)='/dtdd']/@href",
            "chunksize": 10,
            "sema": 10,
        },
        {
            "site": "https://fptshop.com.vn/",
            "xpath": "//a[substring(@href,1,11)='/dien-thoai'or substring(@href,1,18)='/may-tinh-xach-tay']/@href",
            "chunksize": 10,
            "sema": 10,
            "headers": {
                "Cookie": "cf_clearance=srIGkxKx9eUfh_IyuI9WnAahQMhYLxDLV_OrZLt7tNE-1755178655-1.2.1.1-Xc5TgHVNQYiWT_quM0"
                "rusLbLGFqPgDzYIhkeiR3VcG86GLE.sHvo1W4e1ZleC20ShAdI1I5qpImkx5akP58br_TUez_ssiNKP1VkI7RD6R5LF3TFy6xhM2UqbcW"
                "kJUEtr1Vd764mpXtlohrtGdinnZanAprOUE9O87IrX8L55zHpRlp24RvK0nNCp8kvrB6k2dy.wszY7tGpKEPYpTR3vbKeq3V1Ggp0tt8oapveC58"
            },
        },
    ]
    processes: list[multiprocessing.Process] = []

    for i in works:
        p = multiprocessing.Process(target=crawling_work, kwargs=i)
        p.start()
        processes.append(p)
        time.sleep(2)  # delay between processes

    for i in processes:
        i.join()


# crawling
# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #
# scraping


def scraping_work(
    urls_source: str,
    target: str,
    chunksize: int,
    sema: int,
):
    urls = [i["url"] for i in csv_reader(urls_source)]
    scraper = Scraper(urls, target=target, save_in="./scripts/webcrawler/scraped")

    asyncio.run(
        scraper.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            delay=random.uniform(0.5, 1.0),
        )
    )

    scraper.reset()


def scraping_process():
    works = [
        {
            "urls_source": "./scripts/webcrawler/crawled/cellphones.csv",
            "target": "cellphones",
            "chunksize": 50,
            "sema": 50,
        },
        {
            "urls_source": "./scripts/webcrawler/crawled/thegioididong.csv",
            "target": "tgdd",
            "chunksize": 10,
            "sema": 10,
        },
        # {
        #     "urls_source": "./scripts/webcrawler/crawled/fptshop.csv",
        #     "target": "fptshop",
        #     "chunksize": 10,
        #     "sema": 10,
        # },
    ]
    processes: list[multiprocessing.Process] = []

    for i in works:
        p = multiprocessing.Process(target=scraping_work, kwargs=i)
        p.start()
        processes.append(p)
        time.sleep(2)  # delay between processes

    for i in processes:
        i.join()


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


@runtime
def main():
    # crawling_process()
    scraping_process()


if __name__ == "__main__":
    main()
