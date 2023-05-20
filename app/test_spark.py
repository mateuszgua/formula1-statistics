import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .appName('test_app') \
        .master('local[2]') \
        .getOrCreate()


def test_spark_dataframe_creation(spark):
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    header = ["Name", "Age"]

    dataframe = spark.createDataFrame(data, header)

    assert dataframe.count() == 3
    assert len(dataframe.columns) == 2

    expected_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    expected_columns = ["Name", "Age"]

    for i, row in enumerate(dataframe.collect()):
        assert row == expected_data[i]

    assert dataframe.columns == expected_columns
