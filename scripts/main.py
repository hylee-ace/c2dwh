import asyncio, os, re
from webcrawler import Crawler, Scraper
from utils import runtime, csv_reader, athena_sql_executor
from datetime import datetime


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


def create_tables_from_files(
    files_location: str, *, database: str, update_partition: bool = False
):
    queries = []
    tables_meta = []

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

    for i in queries:
        table = re.search(r"table (?:if not exists)?\s*(.*?)\s*\(", i, re.IGNORECASE)
        bucket = re.search(r"location\s*'(s3://.*?)'", i, re.IGNORECASE)
        tables_meta.append(
            (table.group(1) if table else "", bucket.group(1) if bucket else "")
        )

    if not queries:
        print("No SQL queries found.")
        return

    print(f"Start creating tables in {database}...")
    for i in range(len(queries)):
        resp = athena_sql_executor(queries[i], database=database)

        if resp.get("query_execution_state") == "SUCCEEDED":
            print(f"Create table {tables_meta[i][0]} successfully.")
        else:
            print(f"Creating table {tables_meta[i][0]} canceled.")

    if update_partition:
        for i in tables_meta:
            resp = athena_sql_executor(
                f"alter table {i[0]} add partition (partition_date = '{datetime.today().date()}') "
                + f"location '{i[1]}date={datetime.today().date()}'",
                database=database,
            )

            if resp.get("query_execution_state") == "SUCCEEDED":
                print(f"Update partition in {i[0]} successfully.")
            else:
                print(f"Updating partition in {i[0]} canceled.")


def bulk_insert_from_files(files_location: str, *, database: str):
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

    for i in queries:
        table = re.search(r"insert into\s+([^\s]*?)\s+", i, re.IGNORECASE)
        tables.append(table.group(1) if table else "")

    if not queries:
        print("No SQL queries found.")
        return

    print(f"Start processing records and updating tables in {database}...")
    for i in range(len(queries)):
        resp = athena_sql_executor(queries[i], database=database)

        if resp.get("query_execution_state") == "SUCCEEDED":
            print(f"Add new processed records into {tables[i]} successfully.")
        else:
            print(f"Updating table {tables[i]} canceled.")


@runtime
def main():
    # crawling_work(upload_to_s3=True)
    # scraping_work(upload_to_s3=True)
    create_tables_from_files("./scripts/sql/silver/create", database="c2dwh_silver")
    # bulk_insert_from_files("./scripts/sql/silver/insert", database="c2dwh_silver")


if __name__ == "__main__":
    main()
