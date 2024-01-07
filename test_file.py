# Import required libraries
import os
import pyspark.sql
import pytest


@pytest.fixture
def spark_fixture():
    spark = pyspark.sql.SparkSession \
        .builder.appName("Testing PySpark Example") \
        .getOrCreate()
    yield spark


def test_driver():
    '''Unit Test para verificar que
        existe la carpeta y el driver.'''
    assert os.path.exists('driver')


def test_datos(spark_fixture):
    '''Unit Test para las funciones que
        crean dataframes, el programa
        debe arrojar siempre dataframes
        que contengan datos.'''

    spark = pyspark.sql.SparkSession \
        .builder.appName("Testing PySpark Example") \
        .getOrCreate()
    sample_data = [{"movie_id": "1", "rating": 4},
                   {"movie_id": "1", "rating": 5},
                   {"movie_id": "2", "rating": 3},
                   {"movie_id": "2", "rating": 4},
                   {"movie_id": "2", "rating": 5}]
    df = spark.createDataFrame(sample_data)
    MENSAJE_ERROR = 'The dataframe cannot be empty'

    # Verifying empty assertions
    assert df.count() > 0, MENSAJE_ERROR
