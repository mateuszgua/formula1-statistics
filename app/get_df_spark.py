import pyspark

from pyspark.sql import SparkSession
from config import Config


config = Config()


def get_df_spark(header, data):
    print(header)
    print(data)
    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    dataframe = spark.createDataFrame(data, header)
    dataframe.show()
    # print(spark)
    return dataframe

    # df = spark.read.format("csv").option("header", "true").option(
    #     "inferSchema", "true").load(path_csv)
    # df.show()

    # spark_df_a = spark.read.option("header", True).option(
    #     "sep", ",").option("inferSchema", True).csv(path=f'{path_csv}')

    # spark_df_a.printSchema()
    # spark_df_a.describe().show()


# bucket = config.AWS_BUCKET
# data_key = 'data/source/circuits.csv'
# data_location = f"s3a://{bucket}/{data_key}"
# spark = SparkSession.builder.getOrCreate()
# df = spark.read.csv(data_location, header='True', inferSchema=True)
# df.limit(5).toPandas()
# print(df)
