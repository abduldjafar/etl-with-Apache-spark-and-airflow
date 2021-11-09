from config.config import Config
from config.initialize import Initialize
from pyspark.sql.functions import (
    explode,
    col,
    to_date,
    unix_timestamp,
    row_number,
    sum,
    format_number,
)
from pyspark.sql.window import Window


def init_config():
    initialize_config = Config()
    return initialize_config


def spark_session():
    initialize = Initialize()
    return initialize.start_spark()


def load_json_files(file_name):
    df = spark_session().read.json(file_name)
    return df


def create_menu_table(df):

    return (
        df.select("menu", "restaurantName")
        .withColumn("menu", explode(col("menu")))
        .withColumn("dishName", col("menu.dishName"))
        .withColumn("price", col("menu.price"))
        .drop("menu")
    )


def create_user_table(df):
    return df.drop("purchaseHistory")


def create_purchase_history_table(df):
    return (
        df.withColumn("purchaseHistory", explode(col("purchaseHistory")))
        .withColumn("dishName", col("purchaseHistory.dishName"))
        .withColumn("restaurantName", col("purchaseHistory.restaurantName"))
        .withColumn("transactionAmount", col("purchaseHistory.transactionAmount"))
        .withColumn("transactionDate", col("purchaseHistory.transactionDate"))
    )


def cleansing_history_table(df):
    return (
        df.withColumn(
            "transactionDate",
            to_date(
                unix_timestamp(
                    col("purchaseHistory.transactionDate"), "MM/dd/yyyy hh:mm a"
                ).cast("timestamp")
            ),
        )
        .drop("purchaseHistory")
        .withColumn(
            "row",
            row_number().over(Window.partitionBy("id").orderBy(col("transactionDate"))),
        )
        .withColumn(
            "historyTransactionAmount",
            sum("transactionAmount").over(
                Window.partitionBy("id").orderBy("transactionDate")
            ),
        )
        .withColumn("cashBalance", col("cashBalance") - col("historyTransactionAmount"))
        .drop("historyTransactionAmount")
        .withColumn("finalCashBalance", format_number("cashBalance", 2))
        .drop("cashBalance")
        .drop("row")
    )


def get_top_10_restaurant_transactions(df):
    return (
        df.groupBy("restaurantName")
        .agg(sum("transactionAmount"))
        .withColumn("total_transactionAmount", col("sum(transactionAmount)"))
        .orderBy(col("total_transactionAmount").desc())
        .withColumn(
            "total_transactionAmount", format_number("total_transactionAmount", 2)
        ).drop("sum(transactionAmount)")
    )


def create_restaurant_table(df):
    return df.drop("menu")


if __name__ == "__main__":
    data_frame = load_json_files("data_set/restaurant_menu_clean.json")
    menu_table = create_menu_table(data_frame)
    restaurant_table = create_restaurant_table(data_frame)

    second_data_frame = load_json_files(
        "data_set/users_with_purchase_history_clean.json"
    )

    user_table = create_user_table(second_data_frame)
    purchase_history_table = create_purchase_history_table(second_data_frame)

    cleaned_purchase_history_table = cleansing_history_table(purchase_history_table)
    top_10_restaurant_transactions = get_top_10_restaurant_transactions(
        cleaned_purchase_history_table
    )

    user_table.show()
    cleaned_purchase_history_table.show()
    top_10_restaurant_transactions.show()
