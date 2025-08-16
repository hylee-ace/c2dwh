from webcrawler import Crawler
from utils import runtime
import asyncio, multiprocessing


def crawling_work(
    site: str, xpath: str, save_path: str, chunksize: int, sema: int, headers=None
):
    crawler = Crawler(site, search=xpath, save_in=save_path)

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            headers=headers,
        )
    )

    crawler.reset()
    print(f"Crawling {site} done.")


@runtime
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
            "save_path": "./scripts/webcrawler/crawled/cellphones.csv",
            "chunksize": 500,
            "sema": 200,
        },
        {
            "site": "https://www.thegioididong.com/",
            "xpath": "//a[substring(@href,1,7)='/laptop'or substring(@href,1,5)='/dtdd']/@href",
            "save_path": "./scripts/webcrawler/crawled/tgdd.csv",
            "chunksize": 20,
            "sema": 5,
        },
        {
            "site": "https://fptshop.com.vn/",
            "xpath": "//a[substring(@href,1,11)='/dien-thoai'or substring(@href,1,18)='/may-tinh-xach-tay']/@href",
            "save_path": "./scripts/webcrawler/crawled/fptshop.csv",
            "chunksize": 20,
            "sema": 5,
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

    for i in processes:
        i.join()


def main():
    crawling_process()


if __name__ == "__main__":
    main()
