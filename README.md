# C2DWH | Personal project

Crawling to Data warehousing (C2DWH) project involves building crawler and scraper from the scratch to explore e-commerce site and developing a simple OLAP system to analyze purchasing trends for tech devices such as smartphones, laptops, etc. The ELT pipeline will operate weekly or monthly to spot the changes in pricing, ratings, and number of reviews. These data can help to determine the best-selling products and understand what attracts consumers to these items based on their specifications.

## Technologies
- Python
- Amazon S3
- Amazon Glue
- Amazon Athena
- dbt
- Apache Airflow
- Docker

## Architecture
![architecture](/docs/assets/architecture.png)

- [**The Gioi Di Dong**](https://www.thegioididong.com/) is the data source which provides plenty of products varying from smartphones, laptops, to accessories like earphones or smartwatches.
- **Amazon S3** represents the data lake for storing CSV files which contain crawled URLs from base site and scraped products information.
- **Amazon Glue** acts like simple data warehouse, managing data catalog (databases, schemas, tables, etc) for **Amazon Athena**.
- **Amazon Athena** serves as query engine on top of **S3**, cleaning staging data and transforming to fact and dimension tables are executed here via SQL scripts.
- **dbt** helps define and organize the data models, managing transformaions and dependencies in **Athena**.
- **Docker** and **Apache Airflow** help to automate the crawling/scraping process as well as the ELT pipeline.

## Schema
![gold-layer](/docs/assets/c2dwh_gold_layer.png)

## Set-up
Read this [**docs**](/docs/demo.md) for more details about how to set up the project.
