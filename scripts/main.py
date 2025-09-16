import asyncio, os, time, re
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader, athena_sql_executor


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
        s3_attrs={"bucket": "crawling-to-dwh", "obj_prefix": "crawled/"},
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
        s3_attrs={"bucket": "crawling-to-dwh", "obj_prefix": "bronze/"},
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


def create_bronze_layer(files_location: str):
    queries = []
    tables = []

    if os.path.isfile(files_location):
        print(f"Invalid path. {files_location} is a file.")
        return
    if not os.path.exists(files_location):
        print(f"No such directory named {files_location}.")
        return

    for root, _, files in os.walk(files_location):
        for i in files:
            if not i.endswith(".sql"):  # only read sql files
                continue
            with open(os.path.join(root, i), "r") as file:
                queries.append(file.read())

    if not queries:
        print("No SQL queries found.")
        return

    print("Start creating BRONZE layer...")
    for i in queries:
        resp = athena_sql_executor(i, database="c2dwh_bronze")
        table = re.search(r"if not exists\s*(.*?)\s*\(", i)

        if resp.get("query_execution_state") == "SUCCEEDED":
            print(f"Create {table.group(1) if table else ''} successfully.")
        else:
            print(f"Creating table {table.group(1) if table else ''} failed.")

        time.sleep(0.2)

    # add partitions


@runtime
def main():
    # crawling_work(upload_to_s3=True)
    # scraping_work(upload_to_s3=True)
    create_bronze_layer(files_location="./scripts/sql/bronze")


if __name__ == "__main__":
    main()
