import asyncio, multiprocessing, random, time
from webcrawler import Crawler, CpsScraper
from utils import runtime


def crawling_work(
    site: str,
    xpath: str,
    dir: str,
    chunksize: int,
    sema: int,
    delay: float = None,
    headers=None,
):
    crawler = Crawler(site, search=xpath, save_in=dir)

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            headers=headers,
            delay=delay,
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
            "dir": "./scripts/webcrawler/crawled",
            "chunksize": 50,
            "sema": 50,
        },
        {
            "site": "https://www.thegioididong.com/",
            "xpath": "//a[substring(@href,1,7)='/laptop'or substring(@href,1,5)='/dtdd']/@href",
            "dir": "./scripts/webcrawler/crawled",
            "chunksize": 10,
            "sema": 10,
            "delay": random.uniform(0.5, 1.0),
        },
        {
            "site": "https://fptshop.com.vn/",
            "xpath": "//a[substring(@href,1,11)='/dien-thoai'or substring(@href,1,18)='/may-tinh-xach-tay']/@href",
            "dir": "./scripts/webcrawler/crawled",
            "chunksize": 10,
            "sema": 10,
            "delay": random.uniform(0.5, 1.0),
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


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


def scraping_work():
    urls = []

    # get urls from file
    with open("./scripts/webcrawler/crawled/cellphones.csv", "r") as file:
        next(file)
        for i in file:
            urls.append(i.split(",")[0])

    scraper = CpsScraper(urls, save_in="./scripts/webcrawler/data")

    asyncio.run(
        scraper.execute(timeout=20.0, chunksize=50, semaphore=asyncio.Semaphore(50))
    )

    print(len(scraper.result))
    scraper.reset()


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


@runtime
def main():
    # crawling_process()
    # time.sleep(320)
    scraping_work()


if __name__ == "__main__":
    main()
