import asyncio, os, re
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader, s3_file_uploader


def crawling_work(upload_to_s3: bool = False):
    include = [
        ("laptop", 7),
        ("dtdd", 5),
        ("dong-ho-thong-minh", 19),
        ("man-hinh-may-tinh", 18),
        ("may-tinh-bang", 14),
        ("tai-nghe", 9),
    ]
    text = " or ".join([f"substring(@href,1,{i[1]})='/{i[0]}'" for i in include])
    crawler = Crawler(
        "https://www.thegioididong.com/",
        search=f"//a[{text}]/@href",
        save_in="./data/crawled",
        upload_to_s3=upload_to_s3,
        s3_attrs={"bucket": "crawling-to-datalake", "obj_prefix": "crawled/"},
    )

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=12,
            semaphore=asyncio.Semaphore(12),
        )
    )

    crawler.reset()


def scraping_work(
    upload_to_s3: bool = False,
):
    urls = [i["url"] for i in csv_reader("./data/crawled/thegioididong_urls.csv")]
    scraper = Scraper(urls, save_in="./data/scraped")

    asyncio.run(
        scraper.execute(
            timeout=20.0,
            chunksize=12,
            semaphore=asyncio.Semaphore(12),
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


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


# @runtime
def main():
    # crawling_work(upload_to_s3=True)
    # scraping_work()

    t = "asdfd sdafdsa fsd nang 572g (asfs) or nang 34.5g dsaf."
    found = re.match(r"asd", t)

    print(found.span())


if __name__ == "__main__":
    main()
