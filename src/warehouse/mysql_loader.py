import os

from dotenv import load_dotenv


# Load environment variables
load_dotenv()


MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


MYSQL_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/"
    f"{MYSQL_DATABASE}"
    "?useSSL=false"
    "&allowPublicKeyRetrieval=true"
    "&serverTimezone=UTC"
)


MYSQL_PROPERTIES = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}


def load_dataframe_to_mysql(
    df,
    table_name,
    mode="append"
):

    print(
        f"\nLoading {table_name} into MySQL..."
    )

    (
        df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table_name)
        .option(
            "user",
            MYSQL_PROPERTIES["user"]
        )
        .option(
            "password",
            MYSQL_PROPERTIES["password"]
        )
        .option(
            "driver",
            MYSQL_PROPERTIES["driver"]
        )
        .mode(mode)
        .save()
    )

    print(
        f"Successfully loaded {table_name}"
    )