import asyncio, logging
from webcrawler import Crawler, Scraper
from utils import csv_reader, runtime


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #
def crawling_work(upload_to_s3: bool = False):
    print("Start crawling process...")

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
        save_in="./src/data/crawled",
        upload_to_s3=upload_to_s3,
        s3_attrs={"bucket": "crawling-to-dwh", "obj_prefix": "crawled/"},
    )

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=10,
            semaphore=asyncio.Semaphore(10),
        )
    )
    crawler.reset()


def scraping_work(upload_to_s3: bool = False):
    print("Start scraping process...")

    urls = [i["url"] for i in csv_reader("./src/data/crawled/thegioididong_urls.csv")]
    scraper = Scraper(
        urls,
        save_in="./src/data/scraped",
        upload_to_s3=upload_to_s3,
        s3_attrs={"bucket": "crawling-to-dwh", "obj_prefix": "bronze/"},
    )

    asyncio.run(
        scraper.execute(
            timeout=20.0,
            chunksize=10,
            semaphore=asyncio.Semaphore(10),
        )
    )
    scraper.reset()


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #
@runtime
def main():
    # crawling_work()
    scraping_work()


if __name__ == "__main__":
    logging.basicConfig(
        format="[%(asctime)s] [%(name)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=10,
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    main()
