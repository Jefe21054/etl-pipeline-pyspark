# Import required libraries
import pyspark
from decouple import config

# Create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "driver/postgresql-42.2.18.jar") \
   .getOrCreate()

# Read data from .env file with python-decouple
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DATABASE_PORT = config('DATABASE_PORT')

url_conexion = f'jdbc:postgresql://{DB_HOST}:{DATABASE_PORT}/{DB_NAME}'
usuario_conexion = f'{DB_USER}'
clave_conexion = f'{DB_PASSWORD}'

# Read table from db using Spark JDBC
movies_df = spark.read \
   .format("jdbc") \
   .option("url", url_conexion) \
   .option("dbtable", "movies") \
   .option("user", usuario_conexion) \
   .option("password", clave_conexion) \
   .option("driver", "org.postgresql.Driver") \
   .load()

# Print the movies_df
print(movies_df.show())

# Stop the Spark session
spark.stop()
