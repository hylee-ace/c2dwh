import asyncio, multiprocessing, random, time, json
from webcrawler import Crawler, CpsScraper
from utils import runtime


def crawling_work(
    site: str,
    xpath: str,
    save_path: str,
    chunksize: int,
    sema: int,
    delay: float = None,
    headers=None,
):
    crawler = Crawler(site, search=xpath, save_in=save_path)

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
            "save_path": "./scripts/webcrawler/crawled/cellphones.csv",
            "chunksize": 50,
            "sema": 50,
        },
        {
            "site": "https://www.thegioididong.com/",
            "xpath": "//a[substring(@href,1,7)='/laptop'or substring(@href,1,5)='/dtdd']/@href",
            "save_path": "./scripts/webcrawler/crawled/tgdd.csv",
            "chunksize": 10,
            "sema": 10,
            "delay": random.uniform(0.5, 1.0),
        },
        {
            "site": "https://fptshop.com.vn/",
            "xpath": "//a[substring(@href,1,11)='/dien-thoai'or substring(@href,1,18)='/may-tinh-xach-tay']/@href",
            "save_path": "./scripts/webcrawler/crawled/fptshop.csv",
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
    urls = [
        "https://cellphones.com.vn/iphone-16-pro-max.html",  # phone
        "https://cellphones.com.vn/laptop-acer-aspire-lite-15-al15-41p-r3u5.html",  # latop
        "https://cellphones.com.vn/nokia-hmd-105-4g.html",  # phone no discount
        "https://cellphones.com.vn/laptop-acer-nitro-v-16-propanel-anv16-41-r36y.html",  # upcoming latop
        "https://cellphones.com.vn/apple-macbook-air-13-m4-10cpu-8gpu-16gb-256gb-2025.html",  # mac
        "https://cellphones.com.vn/laptop-dell-latitude-e7470.html",  # laptop no price
        "https://cellphones.com.vn/dien-thoai-masstel-izi-16-4g.html",  # unknown genre
        "https://cellphones.com.vn/nubia-neo-2.html",  # unknown genre
        "https://cellphones.com.vn/laptop-hp-omnibook-x-14-fe1010qu-b53kbpa-copilot-x-plus.html",
        "https://cellphones.com.vn/laptop-lenovo-thinkpad-e16-gen-1-21jn006gvn.html",
    ]
    scraper = CpsScraper(urls)

    asyncio.run(scraper.execute())
    for i in scraper.result:
        print(json.dumps(i, indent=2, ensure_ascii=False))
        print("-----------")
    scraper.reset()


@runtime
def main():
    # crawling_process()
    scraping_work()


if __name__ == "__main__":
    main()
