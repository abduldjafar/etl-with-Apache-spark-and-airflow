from config.config import Config
from config.initialize import Initialize
from pyspark.sql.functions import explode, col, regexp_replace


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


def create_restaurant_table(df):
    return df.drop("menu")


if __name__ == "__main__":
    data_frame = load_json_files("data_set/restaurant_menu_clean.json")
    menu_table = create_menu_table(data_frame)
    restaurant_table = create_restaurant_table(data_frame)

    restaurant_table.show()
