import asyncio
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader


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
            chunksize=11,
            semaphore=asyncio.Semaphore(11),
        )
    )
    crawler.reset()


def scraping_work(upload_to_s3: bool = False):
    urls = [i["url"] for i in csv_reader("./data/crawled/thegioididong_urls.csv")]
    scraper = Scraper(
        urls,
        save_in="./data/scraped",
        upload_to_s3=upload_to_s3,
        s3_attrs={"bucket": "crawling-to-datalake", "obj_prefix": "scraped/"},
    )

    asyncio.run(
        scraper.execute(
            timeout=20.0,
            chunksize=11,
            semaphore=asyncio.Semaphore(11),
        )
    )
    scraper.reset()


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


@runtime
def main():
    # crawling_work(upload_to_s3=True)
    scraping_work(upload_to_s3=True)


if __name__ == "__main__":
    main()
