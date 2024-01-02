# Import required libraries
import os
import pyspark.sql
from pyspark.sql.functions import col

# Create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName(" Testing Python Spark SQL ") \
    .config('spark.driver.extraClassPath', "driver/postgresql-42.2.18.jar") \
    .getOrCreate()

data = [("1", "4"), ("1", "5"), ("2", "3"), ("2", "4"), ("2", "5")]
columns = ["movie_id", "rating"]
users_df = spark.createDataFrame(data, columns)


def test_driver():
    ''' Unit Test para verificar que 
        existe la carpeta y el driver. '''
    assert os.path.exists('driver')


def test_datos(users_df):
    ''' Unit Test para las funciones que
        crean dataframes, el programa 
        debe arrojar siempre dataframes 
        que contengan datos. '''

    df = users_df
    MENSAJE_ERROR = 'The dataframe cannot be empty'

    # Verifying empty assertions
    assert df.count() > 0, MENSAJE_ERROR

# Stop the Spark session
spark.stop()
