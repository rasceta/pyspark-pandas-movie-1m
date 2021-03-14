import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

now = time.time()
POSTGRESQL_JDBC_PATH = "/usr/share/java/postgresql-42.2.19.jar" # Change this path to your own
spark = SparkSession.builder.appName("PysparkMovies").config("spark.jars", POSTGRESQL_JDBC_PATH).getOrCreate()
print(f"Creating spark session time: {time.time() - now} s\n")

# Create schema when reading ratings.dat
ratingSchema = StructType([ \
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("time", IntegerType(), True)]
)

# Create schema when reading movies.dat
moviesSchema = StructType([ \
    StructField("movieID", IntegerType(), True), \
    StructField("movieTitle", StringType(), True), \
    StructField("genres", StringType(), True)]
)

# Create a user-defined function to get rating in percent
def getRatingPercent(rating):
    return rating/5 * 100
getRatingPercentUDF = func.udf(getRatingPercent)

start_time = time.time()

now = time.time()
# Load up movie data as dataframe
ratingsDF = spark.read.option("sep", "::").schema(ratingSchema).csv("ml-1m/ratings.dat")
print(f"Loading ratings data: {time.time() - now} s")
print(f"Ratings data length: {ratingsDF.count()}\n")

now = time.time()
moviesDF = spark.read.option("sep", "::").schema(moviesSchema).csv("ml-1m/movies.dat")
print(f"Loading movies data: {time.time() - now} s")
print(f"Movies data length: {moviesDF.count()}\n")

now = time.time()
# Creating ratingPercent column on ratings dataframe
ratingsDF = ratingsDF.withColumn("ratingPercent", getRatingPercentUDF(func.col("rating")).cast(IntegerType()))
print(f"Creating rating percent column in ratings dataframe: {time.time() - now} s\n")

now = time.time()
# Joining ratings and movies dataframes
ratingsDF = ratingsDF.alias('ratings')
moviesDF = moviesDF.alias('movies')
joinedDF = ratingsDF.join(moviesDF, func.col('ratings.movieID') == func.col('movies.movieID')) \
            .select('ratings.*', 'movies.movieTitle', 'movies.genres')
print(f"Joining ratings and movies dataframes: {time.time() - now} s\n")

now = time.time()
# Creating rating average per movie dataframe
ratingsAvgDF = ratingsDF["movieID","ratingPercent"].groupBy("movieID").avg()
print(f"Creating rating average per movieID dataframe: {time.time() - now} s\n")

now = time.time()
joinedDF.repartition(10) \
    .write.format('jdbc').options(
        url='jdbc:postgresql://localhost:5432/iykra',
        driver='org.postgresql.Driver',
        dbtable='ratings_spark',
        user='postgres',
        password='postgres'
    ).save()
print(f"Writing joined dataframe to postgresql: {time.time() - now} s\n")

print(f"Total time: {time.time() - start_time} s\n")

print("Stopping Spark session ...")
spark.stop()
print("Spark session stopped")