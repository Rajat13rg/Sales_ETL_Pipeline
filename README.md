Sales Data ETL & Analytics Pipeline

A production-style data engineering project that ingests raw sales data from CSV and JSON files, cleans and transforms the data using PySpark, loads it into a MySQL star-schema data warehouse, and performs analytical SQL queries.

The pipeline also supports incremental loading to prevent duplicate records when the ETL process is executed multiple times.

Project Architecture

CSV + JSON Raw Data
        |
        v
   PySpark ETL
        |
        +--> Data Quality Checks
        +--> Cleaning & Validation
        +--> Transformations & Joins
        |
        v
     Star Schema
        |
        v
 MySQL Data Warehouse
        |
        v
    SQL Analytics

Tech Stack

Technology

Purpose

Python

ETL application

PySpark

Data ingestion, cleaning and transformation

SQL

Analytics and validation

MySQL

Data warehouse

MySQL Workbench

Database management

JDBC

PySpark-to-MySQL connectivity

python-dotenv

Environment configuration

Git/GitHub

Version control

Power BI is optional and can be added as a visualization layer.

Project Structure

Sales_ETL_Analytics/
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── orders.csv
│   │   └── products.json
│   └── processed/
├── src/
│   ├── main.py
│   └── warehouse/
│       ├── __init__.py
│       └── mysql_loader.py
├── sql/
│   └── analytics.sql
├── notebooks/
├── screenshots/
├── .env
├── .gitignore
├── requirements.txt
└── README.md

1. Data Sources

The pipeline reads three raw datasets.

Customers — customers.csv

Contains:

Customer ID

Customer name

Email

City

State

Products — products.json

Contains:

Product ID

Product name

Category

Price

Orders — orders.csv

Contains:

Order ID

Customer ID

Product ID

Quantity

Order date

Payment method

The raw data intentionally contains dirty records to demonstrate a realistic ETL workflow.

2. Data Quality Checks

The pipeline checks for:

Customers

Duplicate customer IDs

Missing names

Missing emails

Missing cities

Products

Invalid/non-positive prices

Duplicate product IDs

Orders

Duplicate order IDs

Missing quantities

Non-positive quantities

Invalid dates

Invalid customer IDs

Invalid product IDs

3. Data Cleaning

Customer Cleaning

Trim whitespace.

Remove duplicate customer IDs.

Replace missing names with Unknown.

Replace missing emails with unknown@example.com.

Replace missing city/state values with Unknown.

Product Cleaning

Trim product names and categories.

Remove products with invalid/non-positive prices.

Remove duplicate product IDs.

Order Cleaning

Remove duplicate orders.

Convert quantity to integer.

Remove non-positive quantities.

Safely parse order dates.

Remove invalid dates.

Standardize payment methods.

Keep only valid customers.

Keep only valid products.

4. Sales Transformation

Orders are joined with customer and product information.

Revenue is calculated as:

Revenue = Quantity × Product Price

The transformed sales dataset contains:

Order ID

Customer information

Product information

Quantity

Unit price

Revenue

Order date

Payment method

5. Data Warehouse — Star Schema

                  dim_customer
                       |
                       |
dim_product ---- fact_sales ---- dim_date

fact_sales

Stores measurable sales transactions:

sales_key
order_id
customer_key
product_key
date_key
quantity
unit_price
revenue
payment_method

dim_customer

customer_key
customer_id
customer_name
email
city
state

dim_product

product_key
product_id
product_name
category
price

dim_date

date_key
full_date
year
quarter
month
month_name
day
day_name

6. Surrogate Keys

The warehouse uses surrogate keys in dimension tables.

Example:

Business ID       Surrogate Key
C001              1
C002              2
C003              3

The fact table stores these surrogate keys to connect sales transactions to their dimensions.

7. Incremental Loading

The fact table uses incremental loading based on order_id.

Before inserting records, the pipeline reads existing order IDs from MySQL and identifies only orders that are not already present.

New Sales Data
      |
      v
Compare order_id
with fact_sales
      |
      +---- Existing ----> Skip
      |
      +---- New ----------> Insert

Example:

First run:
64 records → loaded

Second run with same data:
0 new records → no duplicate insertion

One new valid order:
64 existing + 1 new
→ 1 record inserted
→ 65 total records

This makes the fact load incremental and idempotent with respect to the order-level business key.

8. MySQL Warehouse

Database:

sales_dw

Tables:

dim_customer
dim_product
dim_date
fact_sales

The fact table uses foreign keys to reference the dimension tables.

9. SQL Analytics

The project includes SQL analysis for:

Overall KPIs

Total customers

Total products

Total sales records

Total orders

Total revenue

Average order value

Product Analysis

Units sold by product

Revenue by product

Product ranking

Top 3 products per category

Category Analysis

Revenue by category

Units sold by category

Customer Analysis

Customer revenue

Customer order count

Customer ranking

Customer value

Time Analysis

Monthly revenue

Daily revenue

Running revenue

Month-over-month revenue

Payment Analysis

Revenue by payment method

Orders by payment method

Geographic Analysis

Revenue by state

Revenue by city

Warehouse Validation

Dimension record counts

Fact record count

Foreign-key validation

10. Current Pipeline Results

The intentionally dirty raw dataset contains:

Customers : 13
Products  : 15
Orders    : 71

After cleaning:

Clean customers : 12
Clean products  : 14
Clean orders    : 64
Sales records   : 64

These values represent the current test dataset used during development.

11. How to Run

1. Clone the repository

git clone <your-github-repository-url>
cd Sales_ETL_Analytics

2. Create a virtual environment

python -m venv venv

Windows:

venv\Scripts\activate

3. Install dependencies

pip install -r requirements.txt

4. Configure .env

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=sales_dw
MYSQL_USER=root
MYSQL_PASSWORD=YOUR_PASSWORD

Do not commit .env to GitHub.

5. Create the database

In MySQL Workbench:

CREATE DATABASE sales_dw;
USE sales_dw;

Create the four warehouse tables using the project's schema.

6. Run the ETL pipeline

From the project root:

python src/main.py

7. Run SQL analytics

Open:

sql/analytics.sql

in MySQL Workbench and execute the queries.

12. Key Engineering Concepts Demonstrated

ETL pipelines

Batch processing

PySpark DataFrames

Data quality

Data validation

Data cleaning

Joins

Aggregations

Data transformation

Star schema

Fact and dimension tables

Surrogate keys

Foreign keys

JDBC

MySQL data warehousing

Analytical SQL

Window functions

Ranking

Running totals

Month-over-month analysis

Incremental loading

Idempotent processing

Environment variables

13. Future Improvements

Possible extensions:

Apache Airflow orchestration

Dockerization

Cloud storage

Cloud data warehouse

Automated data-quality tests

Logging and monitoring

Slowly Changing Dimensions (SCD)

Full calendar date dimension

Power BI dashboard

CI/CD

Automated unit and integration testing

These are optional extensions and are not required for the current implementation.

14. Screenshots

screenshots/
├── data_quality_report.png
├── mysql_star_schema.png
├── sql_analytics.png
└── incremental_load.png


15. Interview Explanation

60-second project explanation

I built a Sales Data ETL and Analytics Pipeline using Python, PySpark, SQL, and MySQL. The pipeline ingests sales data from CSV and JSON sources, performs data-quality checks and cleaning using PySpark, transforms the data into a sales dataset, and loads it into a MySQL star-schema data warehouse containing customer, product, date, and sales fact tables. I also implemented incremental loading using existing order IDs to prevent duplicate fact records when the pipeline is rerun. Finally, I created SQL analytics for revenue, products, customers, categories, payment methods, and time-based trends.

Why PySpark?

PySpark provides DataFrame-based processing for large-scale ETL workloads and provides APIs for cleaning, joining, transforming and aggregating structured data.

Why a star schema?

A star schema separates measurable business events from descriptive dimensions, making analytical queries easier to understand and maintain.

How are duplicates prevented?

Existing order IDs are read from the warehouse and compared with incoming data using a left-anti join. Only orders that do not already exist in the fact table are inserted.

16. Author

Rajat Goyal

Computer Engineering Student

Python · PySpark · SQL · MySQL · ETL · Data Warehousing

License

This project is intended for educational, portfolio, and interview purposes.