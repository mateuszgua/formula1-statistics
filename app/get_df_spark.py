import pyspark

from pyspark.sql import SparkSession
from config import Config


config = Config()


def get_df_spark(header, data):
    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    dataframe = spark.createDataFrame(data, header)
    return dataframe
