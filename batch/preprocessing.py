import os
import sys
import findspark
findspark.init("/home/mohamedehab/spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, LongType
)

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *


def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME + "_Preprocessing") \
        .master(SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_ratings(spark):
    schema = StructType([
        StructField("user_id",   IntegerType(), False),
        StructField("movie_id",  IntegerType(), False),
        StructField("rating",    FloatType(),   False),
        StructField("timestamp", LongType(),    False),
    ])
    df = spark.read \
        .option("sep", "::") \
        .option("header", "false") \
        .schema(schema) \
        .csv(RATINGS_FILE)
    return df


def load_movies(spark):
    df = spark.read \
        .option("sep", "::") \
        .option("header", "false") \
        .csv(MOVIES_FILE) \
        .toDF("movie_id", "title", "genres") \
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
    return df


def clean_ratings(df):
    print("\n[PREPROCESSING] Raw record count:", df.count())

    df = df.dropna()

    df = df.filter((F.col("rating") >= 1.0) & (F.col("rating") <= 5.0))

    df = df.filter((F.col("user_id") > 0) & (F.col("movie_id") > 0))

    from pyspark.sql.window import Window
    window = Window.partitionBy("user_id", "movie_id").orderBy(F.col("timestamp").desc())
    df = df.withColumn("rank", F.row_number().over(window)) \
           .filter(F.col("rank") == 1) \
           .drop("rank")

    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"))
    )

    print("[PREPROCESSING] Clean record count:", df.count())
    return df


def show_statistics(df, movies_df):
    print("\n" + "="*55)
    print("        DATASET STATISTICS")
    print("="*55)
    print(f"  Total ratings   : {df.count():,}")
    print(f"  Unique users    : {df.select('user_id').distinct().count():,}")
    print(f"  Unique movies   : {df.select('movie_id').distinct().count():,}")
    print(f"  Rating min      : {df.agg(F.min('rating')).collect()[0][0]}")
    print(f"  Rating max      : {df.agg(F.max('rating')).collect()[0][0]}")
    print(f"  Rating avg      : {df.agg(F.avg('rating')).collect()[0][0]:.4f}")

    total_possible = df.select('user_id').distinct().count() * \
                     df.select('movie_id').distinct().count()
    sparsity = 1 - (df.count() / total_possible)
    print(f"  Matrix sparsity : {sparsity*100:.2f}%")

    print("\n  Rating Distribution:")
    df.groupBy("rating").count().orderBy("rating").show()
    print("="*55 + "\n")


def preprocess():
    spark = create_spark_session()

    ratings_df = load_ratings(spark)
    movies_df  = load_movies(spark)
    clean_df   = clean_ratings(ratings_df)

    show_statistics(clean_df, movies_df)

    print("[PREPROCESSING] Complete. Returning clean DataFrame.")
    return spark, clean_df, movies_df


if __name__ == "__main__":
    spark, clean_df, movies_df = preprocess()
    clean_df.show(10)
    spark.stop()
