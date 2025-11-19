FROM apache/airflow:latest


COPY ./src/profiles.yml /home/airflow/.dbt/
COPY ./src /tmp/src
COPY ./pyproject.toml /tmp

# grant permission to install package
USER root
RUN chown -R airflow /tmp
USER airflow

RUN cd /tmp && pip install --no-cache-dir --user .
RUN rm -rf /tmp/src /tmp/build /tmp/pyproject.toml
