FROM apache/airflow:latest

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY ./src/profiles.yml /home/airflow/.dbt/