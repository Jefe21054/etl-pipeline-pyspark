# Import required libraries
import os
import pyspark.sql

# Create spark session
spark = pyspark.sql.SparkSession.builder \
    .appName(" Testing Python Spark SQL ") \
    .getOrCreate()

sample_data = [{"movie_id": "1", "rating": 4},
               {"movie_id": "1", "rating": 5},
               {"movie_id": "2", "rating": 3},
               {"movie_id": "2", "rating": 4},
               {"movie_id": "2", "rating": 5}]
dataframe = spark.createDataFrame(sample_data)


def test_driver():
    '''Unit Test para verificar que
        existe la carpeta y el driver.'''
    assert os.path.exists('driver')


def test_datos(dataframe):
    '''Unit Test para las funciones que
        crean dataframes, el programa
        debe arrojar siempre dataframes
        que contengan datos.'''

    df = dataframe
    MENSAJE_ERROR = 'The dataframe cannot be empty'

    # Verifying empty assertions
    assert df.count() > 0, MENSAJE_ERROR


# Stop the Spark session
spark.stop()
