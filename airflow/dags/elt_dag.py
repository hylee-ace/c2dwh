import pendulum, asyncio, os, re, logging, time
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from datetime import datetime, timedelta
from utils import csv_reader, athena_sql_executor
from webcrawler import Crawler
from webcrawler import Scraper


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
log = logging.getLogger("airflow")
logging.getLogger("asyncio").setLevel(logging.WARNING)


def crawling_work(upload_to_s3: bool = False):
    log.info("Start crawling process...")

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
        save_in="/home/data/crawled",
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
    log.info("Start scraping process...")

    urls = [
        i["url"]
        for i in csv_reader("/home/data/crawled/thegioididong_urls.csv", logger=log)
    ]
    scraper = Scraper(
        urls,
        save_in="/home/data/scraped",
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


def check_records():
    res = csv_reader("/home/data/crawled/thegioididong_urls.csv", logger=log)

    if not res:  # empty file
        log.info("End the pipeline.")
        return False

    for i in res:
        if (
            i.get("created_at")
            and datetime.fromisoformat(i.get("created_at")).date()
            == datetime.today().date()
        ):
            log.info("Conditions passed. Start scraping after 3 min.")
            time.sleep(180)  # prevent ip banned
            return True

    log.info("End the pipeline.")
    return False


def build_bronze_layer():
    files_location = "/opt/airflow/plugins/sql"
    database = "c2dwh_bronze"
    queries = []
    tables_meta = []

    if os.path.isfile(files_location):
        log.error(f"Invalid path. {files_location} is a file.")
        return
    if not os.path.exists(files_location):
        log.error(f"No such directory named {files_location}.")
        return

    for root, _, files in os.walk(files_location):
        for i in files:
            if not i.endswith(".sql"):  # only read sql files
                continue
            with open(os.path.join(root, i), "r") as file:
                queries.append(file.read())

    # extract metadata
    for i in queries:
        table = re.search(r"if not exists\s*(.*?)\s*\(", i)
        bucket = re.search(r"location\s*'(s3://.*?)'", i)
        tables_meta.append(
            (table.group(1) if table else "", bucket.group(1) if bucket else "")
        )

    if not queries:
        log.warning("No SQL queries found.")
        return

    log.info(f"Start creating tables in {database}...")
    for i in range(len(queries)):
        resp = athena_sql_executor(queries[i], database=database, logger=log)

        if resp.get("query_execution_state") == "SUCCEEDED":
            log.info(f"Create {tables_meta[i][0]} successfully.")
        else:
            log.error(f"Cannot create table {tables_meta[i][0]}.")

    # update partitions
    log.info("Start adding tables partition...")
    for i in tables_meta:
        resp = athena_sql_executor(
            f"alter table {i[0]} add partition (partition_date = '{datetime.today().date()}') "
            + f"location '{i[1]}date={datetime.today().date()}'",
            database=database,
            logger=log,
        )

        if resp.get("query_execution_state") == "SUCCEEDED":
            log.info(f"Update partition in {i[0]} successfully.")
        else:
            log.warning(f"Cannot update partition in {i[0]}.")


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


with DAG(
    "c2dwh_elt_pipeline",
    description="Pipeline from crawling to building OLAP on Glue",
    default_args={
        "owner": "Hy Le",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    start_date=datetime(2025, 9, 22, 7, 0, tzinfo=local_tz),
    end_date=datetime(2025, 12, 31, 7, 0, tzinfo=local_tz),
    schedule="0 8 * SEP-DEC SUN",  # dont set when manually trigger
    catchup=False,
):
    # extract
    crawling_task = PythonOperator(
        task_id="crawl_urls",
        python_callable=crawling_work,
        op_args=[True],
        retries=3,
        retry_delay=timedelta(minutes=3),
    )
    scraping_task = PythonOperator(
        task_id="scrape_urls",
        python_callable=scraping_work,
        op_args=[True],
        retries=3,
        retry_delay=timedelta(minutes=3),
    )
    checker = ShortCircuitOperator(task_id="check_urls", python_callable=check_records)

    # load
    bronze_tier_task = PythonOperator(
        task_id="build_staging_layer",
        python_callable=build_bronze_layer,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,  # wait longer after each retry
    )

    # transform
    silver_tier_task = BashOperator(
        task_id="clean_staging_data",
        bash_command="cd /home/dbt_c2dwh && dbt run -s models/silver",
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    gold_tier_task = BashOperator(
        task_id="build_olap_schema",
        bash_command="cd /home/dbt_c2dwh && dbt run -s models/gold",
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    creating_marts_task = BashOperator(
        task_id="create_data_marts",
        bash_command="cd /home/dbt_c2dwh && dbt run -s models/marts && dbt clean",
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    # workflow
    crawling_task >> checker >> scraping_task >> bronze_tier_task
    bronze_tier_task >> silver_tier_task >> gold_tier_task >> creating_marts_task
