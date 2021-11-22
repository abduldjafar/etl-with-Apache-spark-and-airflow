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
    max,
    min,
    when,
    year,
    month,
    lit,
    concat,
    split,
    size,
    trim,
    upper,
    length,
    to_timestamp,
    regexp_replace,
)
from pyspark.sql.window import Window


def init_config():
    initialize_config = Config()
    return initialize_config


def spark_session():
    initialize = Initialize()
    return initialize.start_spark(
        spark_config={"spark.jars": "jars/neo4j-connector.jar"}
    )


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
    return get_restaurant_transactions_amount(df).limit(10)


def get_restaurant_transactions_amount(df):
    return (
        df.groupBy("restaurantName")
        .agg(sum("transactionAmount"))
        .withColumn("total_transactionAmount", col("sum(transactionAmount)"))
        .orderBy(col("total_transactionAmount").desc())
        .withColumn(
            "total_transactionAmount", format_number("total_transactionAmount", 2)
        )
        .drop("sum(transactionAmount)")
        .withColumnRenamed("restaurantName", "restaurant_name")
    )


def get_max_min_date(df):
    return (
        df.select(min("transactionDate"), max("transactionDate"))
        .limit(1)
        .withColumn("min_date", col("min(transactionDate)"))
        .withColumn("max_date", col("max(transactionDate)"))
        .drop("min(transactionDate)")
        .drop("max(transactionDate)")
    )


def get_amount_transaction_every_day(df):
    return (
        df.groupBy("restaurantName").pivot("transactionDate").sum("transactionAmount")
    )


def get_amount_transaction_every_mounth(df):
    return (
        df.withColumn("year", year("transactionDate").cast("string"))
        .withColumn("month", month("transactionDate").cast("string"))
        .withColumn("year_month", concat(col("year"), lit("-"), col("month")))
        .drop("year")
        .drop("month")
        .groupBy("restaurantName")
        .pivot("year_month")
        .sum("transactionAmount")
    )


def get_amount_transaction_every_year(df):
    return (
        df.withColumn("year", year("transactionDate").cast("string"))
        .groupBy("restaurantName")
        .pivot("year")
        .sum("transactionAmount")
    )


def create_restaurant_table(df):
    return df.drop("menu")


def cleaning_restaurant_table(df):
    return (
        df.withColumn("openingHours", split(col("openingHours"), "/"))
        .withColumn("openingHours", explode(col("openingHours")))
        .withColumn("openingHours", split(col("openingHours"), ","))
        .withColumn(
            "openingHours",
            when(
                size(col("openingHours")) == 2,
                concat(
                    col("openingHours")[0],
                    lit(" "),
                    split(col("openingHours")[1], " ")[2],
                    lit(" "),
                    split(col("openingHours")[1], " ")[3],
                    lit(" "),
                    split(col("openingHours")[1], " ")[4],
                    lit(" "),
                    split(col("openingHours")[1], " ")[5],
                    lit(" "),
                    split(col("openingHours")[1], " ")[6],
                    lit(","),
                    col("openingHours")[1],
                ),
            ).otherwise(col("openingHours")[0]),
        )
        .withColumn("openingHours", split(col("openingHours"), ","))
        .withColumn("openingHours", explode(col("openingHours")))
        .withColumn("openingHours", trim(col("openingHours")))
        .withColumn("day", split(col("openingHours"), " ")[0])
        .withColumn(
            "open",
            concat(
                split(col("openingHours"), " ")[1],
                lit(" "),
                split(col("openingHours"), " ")[2],
            ),
        )
        .withColumn(
            "close",
            concat(
                split(col("openingHours"), " ")[4],
                lit(" "),
                split(col("openingHours"), " ")[5],
            ),
        )
        .drop("openingHours")
        .drop("cashBalance")
        .withColumn(
            "open",
            when(length(split(col("open"), ":")[0]) > 1, col("open")).otherwise(
                concat(
                    lit("0"),
                    split(col("open"), ":")[0],
                    lit(":"),
                    split(col("open"), ":")[1],
                )
            ),
        )
        .withColumn(
            "close",
            when(length(split(col("close"), ":")[0]) > 1, col("close")).otherwise(
                concat(
                    lit("0"),
                    split(col("close"), ":")[0],
                    lit(":"),
                    split(col("close"), ":")[1],
                )
            ),
        )
        .withColumn("open", upper(col("open")))
        .withColumn("close", upper(col("close")))
        .withColumn(
            "open",
            when(
                size(split(col("open"), ":")) > 1,
                concat(lit("01/01/2020 "), col("open")),
            ).otherwise(
                concat(
                    lit("01/01/2020 "),
                    when(length(split(col("open"), " ")[0]) > 1, "").otherwise(
                        lit("0")
                    ),
                    split(col("open"), " ")[0],
                    lit(":00 "),
                    split(col("open"), " ")[1],
                )
            ),
        )
        .withColumn("open_in_24_hours", to_timestamp("open", "MM/dd/yyyy hh:mm a"))
        .withColumn(
            "close",
            when(
                size(split(col("close"), ":")) > 1,
                concat(lit("01/01/2020 "), col("close")),
            ).otherwise(
                concat(
                    lit("01/01/2020 "),
                    when(length(split(col("close"), " ")[0]) > 1, "").otherwise(
                        lit("0")
                    ),
                    split(col("close"), " ")[0],
                    lit(":00 "),
                    split(col("close"), " ")[1],
                )
            ),
        )
        .withColumn("close_in_24_hours", to_timestamp("close", "MM/dd/yyyy hh:mm a"))
        .withColumn(
            "total_hours",
            col("close_in_24_hours").cast("long")
            - col("open_in_24_hours").cast("long"),
        )
        .withColumn(
            "close",
            when(
                col("total_hours") < 0,
                regexp_replace("close", "01/01/2020", "01/02/2020"),
            ).otherwise(col("close")),
        )
        .withColumn("close_in_24_hours", to_timestamp("close", "MM/dd/yyyy hh:mm a"))
        .withColumn(
            "total_hours_unix",
            (
                unix_timestamp(col("close_in_24_hours"))
                - unix_timestamp(col("open_in_24_hours"))
            )
            / lit(3600),
        )
        .withColumn(
            "total_hours",
            (col("close_in_24_hours")) - (col("open_in_24_hours")),
        )
        .drop("close_in_24_hours")
        .drop("open_in_24_hours")
    )


def get_avg_hours_restaurant_open_weekly(df):
    return (
        df.groupBy("restaurantName")
        .avg("total_hours_unix")
        .withColumn("avg_total_hours_in_week", col("avg(total_hours_unix)"))
        .withColumn(
            "avg_total_hours_in_week", format_number("avg_total_hours_in_week", 2)
        )
        .orderBy(col("avg_total_hours_in_week").desc())
        .drop("avg(total_hours_unix)")
    )


def get_avg_total_hours_with_tot_trx(df1, df2):
    return (
        df1.join(
            df2,
            df1.restaurant_name == df2.restaurantName,
            "inner",
        )
        .drop("restaurantName")
        .orderBy(col("total_transactionAmount").desc())
    )


def write_to_neo4j(df, query):
    df.write.format("org.neo4j.spark.DataSource").mode("overwrite").option(
        "url", "neo4j+s://0a495e9c.databases.neo4j.io:7687"
    ).option("authentication.type", "basic").option(
        "authentication.basic.username", "neo4j"
    ).option(
        "authentication.basic.password", "LExlNbUp5abBbShOlWnFLk_05LJ4_nCuWdbuCidzHmk"
    ).option(
        "query", query
    ).save()


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

    restaurant_transactions_amount = get_restaurant_transactions_amount(
        cleaned_purchase_history_table
    )

    amount_transaction_every_day = get_amount_transaction_every_day(
        cleaned_purchase_history_table
    )
    amount_transaction_every_mounth = get_amount_transaction_every_mounth(
        cleaned_purchase_history_table
    )
    amount_transaction_every_year = get_amount_transaction_every_year(
        cleaned_purchase_history_table
    )

    cleaned_restaurant_table = cleaning_restaurant_table(restaurant_table)
    avg_hours_restaurant_open_weekly = get_avg_hours_restaurant_open_weekly(
        cleaned_restaurant_table
    )

    avg_total_hours_with_tot_trx = get_avg_total_hours_with_tot_trx(
        restaurant_transactions_amount, avg_hours_restaurant_open_weekly
    )

    user_table.show()
    purchase_history_table.show()

    write_to_neo4j(restaurant_table, "CREATE (:Restaurant {name:event.restaurantName})")
    write_to_neo4j(
        user_table.select("name").distinct(), "CREATE (:Customer {name:event.name})"
    )
    write_to_neo4j(
        purchase_history_table.select("dishName").distinct(),
        "CREATE (:Dishes {name:event.dishName})",
    )
    write_to_neo4j(
        purchase_history_table,
        "MATCH (customer:Customer {name: event.name}) "
        + "MATCH (restaurant:Restaurant {name: event.restaurantName}) "
        + "MATCH (dishes:Dishes {name: event.dishName}) "
        + "CREATE (customer)-[r:has_buy]->(dishes)->[b:buy_at]->(restaurant)",
    )
