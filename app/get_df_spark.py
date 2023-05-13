import pyspark

from pyspark.sql import SparkSession
from config import Config


config = Config()


def get_df_spark(header, data):
    try:
        spark = SparkSession.builder.appName('sparkdf').getOrCreate()
        dataframe = spark.createDataFrame(data, header)
    except:
        print(f"Error with create dataframe")
    else:
        return dataframe
