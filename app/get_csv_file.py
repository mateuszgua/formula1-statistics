from pyspark.sql import SparkSession


def get_csv_file(path_csv):
    spark = SparkSession.builder.getOrCreate()
    print(spark)

    spark_df_a = spark.read \
        .option("header", False) \
        .option("sep", ",") \
        .option("inferSchema", True) \
        .csv(path=f'{path_csv}')

    spark_df_a.printSchema()
    spark_df_a.describe().show()
