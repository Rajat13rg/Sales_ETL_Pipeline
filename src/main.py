import os

# Hadoop configuration for Windows
os.environ["HADOOP_HOME"] = r"C:/hadoop"
os.environ["hadoop.home.dir"] = r"C:/hadoop"
os.environ["PATH"] = r"C:/hadoop/bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    try_to_date,
    year,
    month,
    quarter,
    date_format,
    dayofmonth
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from warehouse.mysql_loader import load_dataframe_to_mysql


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SalesDataETL")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "com.mysql:mysql-connector-j:9.4.0"
        )
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.executor.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# 2. LOAD RAW DATA
# ============================================================

def load_data(spark):

    print("\n========== LOADING RAW DATA ==========\n")

    # --------------------------------------------------------
    # CUSTOMERS
    # --------------------------------------------------------

    customers_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/customers.csv")
    )


    # --------------------------------------------------------
    # PRODUCTS
    # --------------------------------------------------------

    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

    products_df = (
        spark.read
        .schema(product_schema)
        .json("data/raw/products.json")
    )


    # --------------------------------------------------------
    # ORDERS
    # --------------------------------------------------------

    orders_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/orders.csv")
    )

    return customers_df, products_df, orders_df


# ============================================================
# 3. DISPLAY RAW DATA
# ============================================================

def inspect_data(
    customers_df,
    products_df,
    orders_df
):

    print("\n========== CUSTOMERS ==========")

    customers_df.printSchema()

    print(
        "Customer records:",
        customers_df.count()
    )

    customers_df.show(
        10,
        truncate=False
    )


    print("\n========== PRODUCTS ==========")

    products_df.printSchema()

    print(
        "Product records:",
        products_df.count()
    )

    products_df.show(
        10,
        truncate=False
    )


    print("\n========== ORDERS ==========")

    orders_df.printSchema()

    print(
        "Order records:",
        orders_df.count()
    )

    orders_df.show(
        10,
        truncate=False
    )


# ============================================================
# 4. DATA QUALITY REPORT
# ============================================================

def data_quality_report(
    customers_df,
    products_df,
    orders_df
):

    print("\n")
    print("=" * 60)
    print("                 DATA QUALITY REPORT")
    print("=" * 60)


    # ========================================================
    # CUSTOMERS
    # ========================================================

    print("\nCUSTOMERS")
    print("-" * 40)

    duplicate_customers = (
        customers_df.count()
        - customers_df
        .dropDuplicates(["customer_id"])
        .count()
    )

    missing_names = customers_df.filter(
        col("name").isNull()
        | (trim(col("name")) == "")
    ).count()

    missing_emails = customers_df.filter(
        col("email").isNull()
        | (trim(col("email")) == "")
    ).count()

    missing_cities = customers_df.filter(
        col("city").isNull()
        | (trim(col("city")) == "")
    ).count()

    print(
        "Total records       :",
        customers_df.count()
    )

    print(
        "Duplicate customers :",
        duplicate_customers
    )

    print(
        "Missing names       :",
        missing_names
    )

    print(
        "Missing emails      :",
        missing_emails
    )

    print(
        "Missing cities      :",
        missing_cities
    )


    # ========================================================
    # PRODUCTS
    # ========================================================

    print("\nPRODUCTS")
    print("-" * 40)

    invalid_prices = products_df.filter(
        col("price") <= 0
    ).count()

    print(
        "Total records       :",
        products_df.count()
    )

    print(
        "Invalid prices      :",
        invalid_prices
    )


    # ========================================================
    # ORDERS
    # ========================================================

    print("\nORDERS")
    print("-" * 40)

    duplicate_orders = (
        orders_df.count()
        - orders_df
        .dropDuplicates(["order_id"])
        .count()
    )

    missing_quantity = orders_df.filter(
        col("quantity").isNull()
    ).count()

    invalid_quantity = orders_df.filter(
        col("quantity") <= 0
    ).count()

    invalid_dates = orders_df.filter(
        try_to_date(
            col("order_date"),
            "yyyy-MM-dd"
        ).isNull()
    ).count()

    print(
        "Total records       :",
        orders_df.count()
    )

    print(
        "Duplicate orders    :",
        duplicate_orders
    )

    print(
        "Missing quantity    :",
        missing_quantity
    )

    print(
        "Invalid quantity    :",
        invalid_quantity
    )

    print(
        "Invalid dates       :",
        invalid_dates
    )

    print("\n" + "=" * 60)


# ============================================================
# 5. CLEAN CUSTOMERS
# ============================================================

def clean_customers(customers_df):

    print("\n========== CLEANING CUSTOMERS ==========")

    df = customers_df


    # Remove whitespace
    df = df.withColumn(
        "name",
        trim(col("name"))
    )

    df = df.withColumn(
        "email",
        trim(col("email"))
    )

    df = df.withColumn(
        "city",
        trim(col("city"))
    )

    df = df.withColumn(
        "state",
        trim(col("state"))
    )


    # Remove duplicate customers
    df = df.dropDuplicates(
        ["customer_id"]
    )


    # Handle missing values
    df = df.fillna({
        "name": "Unknown",
        "email": "unknown@example.com",
        "city": "Unknown",
        "state": "Unknown"
    })


    return df


# ============================================================
# 6. CLEAN PRODUCTS
# ============================================================

def clean_products(products_df):

    print("\n========== CLEANING PRODUCTS ==========")

    df = products_df


    # Remove whitespace
    df = df.withColumn(
        "product_name",
        trim(col("product_name"))
    )

    df = df.withColumn(
        "category",
        trim(col("category"))
    )


    # Keep only valid positive prices
    df = df.filter(
        col("price") > 0
    )


    # Remove duplicate products
    df = df.dropDuplicates(
        ["product_id"]
    )


    return df


# ============================================================
# 7. CLEAN ORDERS
# ============================================================

def clean_orders(
    orders_df,
    customers_df,
    products_df
):

    print("\n========== CLEANING ORDERS ==========")

    df = orders_df


    # --------------------------------------------------------
    # Remove duplicate orders
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["order_id"]
    )


    # --------------------------------------------------------
    # Convert quantity to integer
    # --------------------------------------------------------

    df = df.withColumn(
        "quantity",
        col("quantity").cast(IntegerType())
    )


    # --------------------------------------------------------
    # Keep only positive quantities
    # --------------------------------------------------------

    df = df.filter(
        col("quantity") > 0
    )


    # --------------------------------------------------------
    # Safely convert order date
    # --------------------------------------------------------

    df = df.withColumn(
        "order_date",
        try_to_date(
            col("order_date"),
            "yyyy-MM-dd"
        )
    )


    # --------------------------------------------------------
    # Remove invalid dates
    # --------------------------------------------------------

    df = df.filter(
        col("order_date").isNotNull()
    )


    # --------------------------------------------------------
    # Standardize payment method
    # --------------------------------------------------------

    df = df.withColumn(
        "payment_method",
        upper(
            trim(
                col("payment_method")
            )
        )
    )


    # --------------------------------------------------------
    # Keep valid customers
    # --------------------------------------------------------

    df = df.join(
        customers_df.select(
            "customer_id"
        ),
        on="customer_id",
        how="inner"
    )


    # --------------------------------------------------------
    # Keep valid products
    # --------------------------------------------------------

    df = df.join(
        products_df.select(
            "product_id"
        ),
        on="product_id",
        how="inner"
    )


    return df


# ============================================================
# 8. CREATE SALES DATASET
# ============================================================

def create_sales_dataset(
    orders_df,
    customers_df,
    products_df
):

    print(
        "\n========== CREATING SALES DATASET =========="
    )


    # --------------------------------------------------------
    # Join orders with customers
    # --------------------------------------------------------

    sales_df = orders_df.join(
        customers_df,
        on="customer_id",
        how="inner"
    )


    # --------------------------------------------------------
    # Join orders with products
    # --------------------------------------------------------

    sales_df = sales_df.join(
        products_df,
        on="product_id",
        how="inner"
    )


    # --------------------------------------------------------
    # Calculate revenue
    # --------------------------------------------------------

    sales_df = sales_df.withColumn(
        "revenue",
        col("quantity") * col("price")
    )


    # --------------------------------------------------------
    # Select final columns
    # --------------------------------------------------------

    sales_df = sales_df.select(
        "order_id",
        "customer_id",
        "name",
        "email",
        "city",
        "state",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "price",
        "revenue",
        "order_date",
        "payment_method"
    )


    return sales_df


# ============================================================
# 9. CREATE CUSTOMER DIMENSION
# ============================================================

def create_customer_dimension(
    customers_df
):

    customer_dim = customers_df.select(
        "customer_id",
        col("name").alias(
            "customer_name"
        ),
        "email",
        "city",
        "state"
    )

    return customer_dim


# ============================================================
# 10. CREATE PRODUCT DIMENSION
# ============================================================

def create_product_dimension(
    products_df
):

    product_dim = products_df.select(
        "product_id",
        "product_name",
        "category",
        "price"
    )

    return product_dim


# ============================================================
# 11. CREATE DATE DIMENSION
# ============================================================

def create_date_dimension(
    sales_df
):

    date_dim = (
        sales_df

        .select("order_date")

        .distinct()

        .withColumn(
            "date_key",
            date_format(
                col("order_date"),
                "yyyyMMdd"
            ).cast("int")
        )

        .withColumn(
            "full_date",
            col("order_date")
        )

        .withColumn(
            "year",
            year("order_date")
        )

        .withColumn(
            "quarter",
            quarter("order_date")
        )

        .withColumn(
            "month",
            month("order_date")
        )

        .withColumn(
            "month_name",
            date_format(
                col("order_date"),
                "MMMM"
            )
        )

        .withColumn(
            "day",
            dayofmonth("order_date")
        )

        .withColumn(
            "day_name",
            date_format(
                col("order_date"),
                "EEEE"
            )
        )

        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_name"
        )
    )

    return date_dim

def read_mysql_table(spark, table_name):
    """
    Read an existing MySQL dimension table into Spark.
    """
    from warehouse.mysql_loader import MYSQL_URL, MYSQL_PROPERTIES

    return (
        spark.read
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table_name)
        .option("user", MYSQL_PROPERTIES["user"])
        .option("password", MYSQL_PROPERTIES["password"])
        .option("driver", MYSQL_PROPERTIES["driver"])
        .load()
    )


def incremental_load_dimension(
    spark,
    df,
    table_name,
    business_key
):
    """
    Incrementally load a dimension table.

    Only records whose business key does not already exist
    in MySQL are inserted.
    """

    print(f"\n========== INCREMENTAL LOAD: {table_name} ==========")

    try:
        existing_df = read_mysql_table(
            spark,
            table_name
        )

        existing_keys = existing_df.select(
            business_key
        ).distinct()

        new_records = (
            df.join(
                existing_keys,
                on=business_key,
                how="left_anti"
            )
        )

    except Exception:
        # If the table does not exist yet, treat the complete
        # dataframe as new data.
        print(
            f"{table_name} does not exist yet. "
            "All records will be treated as new."
        )
        new_records = df

    new_count = new_records.count()

    print(
        f"New {table_name} records to load:",
        new_count
    )

    if new_count > 0:
        load_dataframe_to_mysql(
            new_records,
            table_name,
            mode="append"
        )
        print(
            f"Loaded {new_count} new records into {table_name}."
        )
    else:
        print(
            f"No new records to load into {table_name}. "
            "Dimension is already up to date."
        )


def incremental_load_fact_sales(
    spark,
    fact_sales
):
    """
    Incrementally load fact_sales using order_id as the
    business key.

    Existing orders are skipped, so running the ETL again
    does not create duplicate fact records.
    """

    print("\n========== INCREMENTAL LOAD: fact_sales ==========")

    try:
        existing_fact = read_mysql_table(
            spark,
            "fact_sales"
        )

        existing_order_ids = (
            existing_fact
            .select("order_id")
            .distinct()
        )

        new_fact_sales = (
            fact_sales.alias("new")
            .join(
                existing_order_ids.alias("existing"),
                on="order_id",
                how="left_anti"
            )
        )

    except Exception:
        # If fact_sales does not exist yet, all fact records
        # are new.
        print(
            "fact_sales does not exist yet. "
            "All records will be treated as new."
        )
        new_fact_sales = fact_sales

    new_count = new_fact_sales.count()

    print(
        "New fact records to load:",
        new_count
    )

    if new_count > 0:
        load_dataframe_to_mysql(
            new_fact_sales,
            "fact_sales",
            mode="append"
        )

        print(
            f"Loaded {new_count} new fact records."
        )
    else:
        print(
            "No new records to load. "
            "fact_sales is already up to date."
        )

    return new_count


def create_fact_sales(spark, sales_df):
    """
    Create fact_sales by replacing business IDs
    with surrogate keys from MySQL dimensions.
    """

    print("\n========== CREATING FACT SALES ==========")

    # Read MySQL dimensions
    customer_dim_mysql = read_mysql_table(
        spark,
        "dim_customer"
    )

    product_dim_mysql = read_mysql_table(
        spark,
        "dim_product"
    )

    date_dim_mysql = read_mysql_table(
        spark,
        "dim_date"
    )

    # Join customer surrogate key
    fact_df = sales_df.join(
        customer_dim_mysql.select(
            "customer_id",
            "customer_key"
        ),
        on="customer_id",
        how="inner"
    )

    # Join product surrogate key
    fact_df = fact_df.join(
        product_dim_mysql.select(
            "product_id",
            "product_key"
        ),
        on="product_id",
        how="inner"
    )

    # Join date surrogate key
    fact_df = fact_df.join(
        date_dim_mysql.select(
            "full_date",
            "date_key"
        ),
        fact_df.order_date == date_dim_mysql.full_date,
        how="inner"
    )

    # Select final fact table columns
    fact_df = fact_df.select(
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "quantity",
        col("price").alias("unit_price"),
        "revenue",
        "payment_method"
    )

    print("\n========== FACT SALES ==========")

    fact_df.show(20, truncate=False)

    print("Fact sales records:", fact_df.count())

    return fact_df

# ============================================================
# 12. MAIN FUNCTION
# ============================================================

def main():

    spark = create_spark_session()

    try:

        # ====================================================
        # EXTRACT
        # ====================================================

        customers_df, products_df, orders_df = (
            load_data(spark)
        )


        # ====================================================
        # INSPECT RAW DATA
        # ====================================================

        inspect_data(
            customers_df,
            products_df,
            orders_df
        )


        # ====================================================
        # DATA QUALITY
        # ====================================================

        data_quality_report(
            customers_df,
            products_df,
            orders_df
        )


        # ====================================================
        # CLEAN CUSTOMERS
        # ====================================================

        clean_customers_df = (
            clean_customers(
                customers_df
            )
        )


        # ====================================================
        # CLEAN PRODUCTS
        # ====================================================

        clean_products_df = (
            clean_products(
                products_df
            )
        )


        # ====================================================
        # CLEAN ORDERS
        # ====================================================

        clean_orders_df = (
            clean_orders(
                orders_df,
                clean_customers_df,
                clean_products_df
            )
        )


        # ====================================================
        # CREATE SALES DATASET
        # ====================================================

        sales_df = (
            create_sales_dataset(
                clean_orders_df,
                clean_customers_df,
                clean_products_df
            )
        )


        print(
            "\n========== SALES DATASET =========="
        )

        sales_df.show(
            20,
            truncate=False
        )


        # ====================================================
        # CREATE DIMENSIONS
        # ====================================================

        print(
            "\n========== CREATING DIMENSIONS =========="
        )


        customer_dim = (
            create_customer_dimension(
                clean_customers_df
            )
        )


        product_dim = (
            create_product_dimension(
                clean_products_df
            )
        )


        date_dim = (
            create_date_dimension(
                sales_df
            )
        )


        # ====================================================
        # DISPLAY DIMENSIONS
        # ====================================================

        print(
            "\n========== CUSTOMER DIMENSION =========="
        )

        customer_dim.show(
            truncate=False
        )


        print(
            "\n========== PRODUCT DIMENSION =========="
        )

        product_dim.show(
            truncate=False
        )


        print(
            "\n========== DATE DIMENSION =========="
        )

        date_dim.show(
            20,
            truncate=False
        )


        # ====================================================
        # LOAD DIMENSIONS INTO MYSQL
        # ====================================================

        print(
            "\n========== LOADING DIMENSIONS TO MYSQL =========="
        )


        # load_dataframe_to_mysql(
        #     customer_dim,
        #     "dim_customer"
        # )


        # load_dataframe_to_mysql(
        #     product_dim,
        #     "dim_product"
        # )


        # load_dataframe_to_mysql(
        #     date_dim,
        #     "dim_date"
        # )

        # ============================================================
        # INCREMENTAL LOAD DIMENSIONS
        # ============================================================

        incremental_load_dimension(
            spark,
            customer_dim,
            "dim_customer",
            "customer_id"
        )

        incremental_load_dimension(
            spark,
            product_dim,
            "dim_product",
            "product_id"
        )

        incremental_load_dimension(
            spark,
            date_dim,
            "dim_date",
            "date_key"
        )

        # ============================================================
        # CREATE AND INCREMENTALLY LOAD FACT TABLE
        # ============================================================

        fact_sales = create_fact_sales(
            spark,
            sales_df
        )

        new_fact_count = incremental_load_fact_sales(
            spark,
            fact_sales
        )

        print("\n========== FACT TABLE LOAD COMPLETE ==========")
        print("Fact records generated by current ETL:", fact_sales.count())
        print("New fact records loaded:", new_fact_count)

        # ====================================================
        # DIMENSION COUNTS
        # ====================================================

        print(
            "\n========== DIMENSION COUNTS =========="
        )

        print(
            "Customer dimension:",
            customer_dim.count()
        )

        print(
            "Product dimension:",
            product_dim.count()
        )

        print(
            "Date dimension:",
            date_dim.count()
        )


        # ====================================================
        # FINAL PIPELINE COUNTS
        # ====================================================

        print(
            "\n========== FINAL PIPELINE COUNTS =========="
        )

        print(
            "Clean customers:",
            clean_customers_df.count()
        )

        print(
            "Clean products:",
            clean_products_df.count()
        )

        print(
            "Clean orders:",
            clean_orders_df.count()
        )

        print(
            "Sales records:",
            sales_df.count()
        )


        print(
            "\n================================================"
        )

        print(
            "       ETL PIPELINE COMPLETED"
        )

        print(
            "================================================"
        )


    finally:

        spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()