import asyncio, multiprocessing, random, time, os
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader, s3_file_uploader
from datetime import datetime


def crawling_work(
    site: str,
    xpath: str,
    chunksize: int,
    sema: int,
    headers: dict = None,
    delay: float = None,
    upload_to_s3: bool = False,
):
    crawler = Crawler(site, search=xpath, save_in="./data/crawled")

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            headers=headers,
            delay=delay,
        )
    )

    if upload_to_s3:
        if len(crawler.result - crawler._Crawler__history) > 0:
            bucket = "crawling-to-datalake"
            filename = os.path.basename(crawler.saving_path)
            key = f"crawled/{filename}"

            print(f"Start uploading {filename} to {bucket}...")
            s3_file_uploader(crawler.saving_path, bucket=bucket, key=key)
            print(f"Uploading {filename} successfully.")
        else:
            print("Uploading cancelled since no more urls found.")

    crawler.reset()


def crawling_process(upload_to_s3: bool = False):
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
            "xpath": f"//a[substring(@href,string-length(@href)-4)='.html' and not({exclude_text}) and contains(@href,'cellphones.com.vn')]/@href",
            "chunksize": 50,
            "sema": 50,
            "upload_to_s3": upload_to_s3,
        },
        {
            "site": "https://www.thegioididong.com/",
            "xpath": "//a[substring(@href,1,7)='/laptop' or substring(@href,1,5)='/dtdd']/@href",
            "chunksize": 10,
            "sema": 10,
            "delay": random.uniform(0.5, 1.0),
            "upload_to_s3": upload_to_s3,
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
    delay: float = None,
    upload_to_s3: bool = False,
):
    urls = [i["url"] for i in csv_reader(urls_source)]
    scraper = Scraper(urls, target=target, save_in="./data/scraped")

    asyncio.run(
        scraper.execute(
            timeout=20.0,
            chunksize=chunksize,
            semaphore=asyncio.Semaphore(sema),
            delay=delay,
        )
    )

    if upload_to_s3:
        bucket = "crawling-to-datalake"
        filename = os.path.basename(scraper.saving_path)
        key = f"scraped/{filename}"

        print(f"Start uploading {filename} to {bucket}...")
        s3_file_uploader(scraper.saving_path, bucket=bucket, key=key)
        print(f"Uploading {filename} successfully.")

    scraper.reset()


def scraping_process(upload_to_s3: bool = False):
    works = [
        {
            "urls_source": "./data/crawled/cellphones.csv",
            "target": "cellphones",
            "chunksize": 50,
            "sema": 50,
            "upload_to_s3": upload_to_s3,
        },
        {
            "urls_source": "./data/crawled/thegioididong.csv",
            "target": "tgdd",
            "chunksize": 10,
            "sema": 10,
            "delay": random.uniform(0.5, 1.0),
            "upload_to_s3": upload_to_s3,
        },
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
    # crawling_process(upload_to_s3=True)
    scraping_process(upload_to_s3=True)


if __name__ == "__main__":
    main()
