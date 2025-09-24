FROM apache/airflow:latest

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY ./prototype/profiles.yml /home/airflow/.dbt/