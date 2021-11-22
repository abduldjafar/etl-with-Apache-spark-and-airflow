"""
initialize.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, getenv
from pyspark.sql import SparkSession


class Initialize(object):
    def __init__(self):
        pass

    def start_spark(self, app_name="my_spark_app", master="local[*]", spark_config={}):

        # detect execution environment
        flag_repl = not (hasattr(__main__, "__file__"))
        flag_debug = "DEBUG" in environ.keys()
        mode = getenv("k8s")

        if not (flag_repl or flag_debug) or mode == "k8s":
            # get Spark session factory
            spark_builder = SparkSession.builder.appName(app_name)
        else:
            # get Spark session factory
            spark_builder = SparkSession.builder.master(master).appName(app_name)

            # add other config params
        if len(spark_config) > 0:
            for key, val in spark_config.items():
                spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()

        return spark_sess