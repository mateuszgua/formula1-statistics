from pyspark.sql import SparkSession


def get_csv_file(path_csv):
    spark = SparkSession.builder.getOrCreate()
    print(spark)

    df = spark.read.format("csv").option("header", "true").option(
        "inferSchema", "true").load(path_csv)
    df.show()

    # spark_df_a = spark.read.option("header", True).option(
    #     "sep", ",").option("inferSchema", True).csv(path=f'{path_csv}')

    # spark_df_a.printSchema()
    # spark_df_a.describe().show()
