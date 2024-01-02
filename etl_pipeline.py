# Import required libraries
import pyspark.sql
from pyspark.sql.functions import col
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
POSTGRESQL_DRIVER = 'org.postgresql.Driver'

url_conexion = f'jdbc:postgresql://{DB_HOST}:{DATABASE_PORT}/{DB_NAME}'
usuario_conexion = f'{DB_USER}'
clave_conexion = f'{DB_PASSWORD}'


# Read movies table from db using Spark JDBC
def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", url_conexion) \
        .option("dbtable", "movies") \
        .option("user", usuario_conexion) \
        .option("password", clave_conexion) \
        .option("driver", POSTGRESQL_DRIVER) \
        .load()
    return movies_df


# Read users table from db using Spark JDBC
def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", url_conexion) \
        .option("dbtable", "users") \
        .option("user", usuario_conexion) \
        .option("password", clave_conexion) \
        .option("driver", POSTGRESQL_DRIVER) \
        .load()
    return users_df


def transform_avg_ratings(movies_df, users_df):
    # Transforming tables
    new_users_df = users_df.withColumn("rating", col("rating").cast("int"))
    avg_rating = new_users_df.groupBy("movies_id").mean("rating")
    transform_df = movies_df.join(
        avg_rating,
        movies_df.id == avg_rating.movies_id
    )
    transform_df = transform_df.drop("movies_id")
    return transform_df


# Load transformed dataframe to the database
def load_df_to_db(transform_df):
    mode = 'overwrite'
    properties = {
                 "user": usuario_conexion,
                 "password": clave_conexion,
                 "driver": POSTGRESQL_DRIVER
                 }
    transform_df.write.jdbc(url=url_conexion,
                            table="avg_ratings",
                            mode=mode,
                            properties=properties
                            )


if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    # Pass the dataframes to the transformation function
    ratings_df = transform_avg_ratings(movies_df, users_df)
    # Load the ratings dataframe
    load_df_to_db(ratings_df)
    # Stop the Spark session
    spark.stop()
