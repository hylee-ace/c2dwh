from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum, logging
from utils import s3_folder_cleaner

log = logging.getLogger()


def test():
    s3_folder_cleaner(bucket='c2dwh-athena-queries')
    


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    "c2dwh_elt_pipeline",
    description="Pipeline from crawling to building OLAP on Athena",
    default_args={
        "owner": "Hy Le",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    start_date=datetime(2025, 9, 1, 7, 0, tzinfo=local_tz),
    end_date=datetime(2025, 12, 31, 7, 0, tzinfo=local_tz),
    schedule="0 22 * SEP-DEC SUN",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="test-py-def",
        dag=dag,
        python_callable=test,
    )

    test2 = BashOperator(task_id="test-bash", dag=dag, bash_command="echo hello word")

    task1
