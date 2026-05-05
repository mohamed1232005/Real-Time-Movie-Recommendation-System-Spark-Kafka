import os
import sys
import time
import findspark
findspark.init("/home/mohamedehab/spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *


def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME + "_RecommendationEngine") \
        .master(SPARK_MASTER) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_model():
    return ALSModel.load(MODEL_DIR)


def load_data(spark):
    ratings_schema = StructType([
        StructField("user_id",   IntegerType(), False),
        StructField("movie_id",  IntegerType(), False),
        StructField("rating",    FloatType(),   False),
        StructField("timestamp", IntegerType(), False),
    ])
    ratings_df = spark.read \
        .option("sep", "::").schema(ratings_schema).csv(RATINGS_FILE)

    movies_df = spark.read \
        .option("sep", "::").csv(MOVIES_FILE) \
        .toDF("movie_id", "title", "genres") \
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))

    return ratings_df, movies_df


def get_user_recommendations(spark, model, user_id,
                              ratings_df, movies_df, n=TOP_N):
    start = time.time()

    known_users = [row["user_id"] for row in
                   ratings_df.select("user_id").distinct().limit(10000).collect()]

    if user_id not in known_users:
        print(f"[ENGINE] User {user_id} not in model — applying cold-start fallback")
        return get_popular_recommendations(spark, ratings_df, movies_df, n)

    already_rated = set(
        row["movie_id"] for row in
        ratings_df.filter(F.col("user_id") == user_id)
                  .select("movie_id").collect()
    )

    user_schema = StructType([StructField("user_id", IntegerType(), False)])
    users_df = spark.createDataFrame([(user_id,)], schema=user_schema)
    recs = model.recommendForUserSubset(users_df, n + len(already_rated))

    latency_ms = (time.time() - start) * 1000

    rec_rows = recs.collect()
    if not rec_rows:
        return get_popular_recommendations(spark, ratings_df, movies_df, n)

    results = []
    for r in rec_rows[0]["recommendations"]:
        if r["movie_id"] not in already_rated:
            title = movies_df.filter(
                F.col("movie_id") == r["movie_id"]
            ).select("title").collect()
            results.append({
                "rank":             len(results) + 1,
                "movie_id":         r["movie_id"],
                "title":            title[0]["title"] if title else "Unknown",
                "predicted_rating": round(r["rating"], 4),
                "latency_ms":       round(latency_ms, 2)
            })
            if len(results) == n:
                break

    return results


def get_popular_recommendations(spark, ratings_df, movies_df, n=TOP_N):
    popular = ratings_df.groupBy("movie_id") \
        .agg(
            F.count("*").alias("num_ratings"),
            F.avg("rating").alias("avg_rating")
        ) \
        .filter(F.col("num_ratings") >= 100) \
        .orderBy(F.col("avg_rating").desc()) \
        .limit(n)

    enriched = popular.join(movies_df, "movie_id") \
                      .select("movie_id", "title", "avg_rating", "num_ratings")

    return [
        {
            "rank":         i + 1,
            "movie_id":     row["movie_id"],
            "title":        row["title"],
            "avg_rating":   round(row["avg_rating"], 4),
            "note":         "cold-start fallback (popular movies)"
        }
        for i, row in enumerate(enriched.collect())
    ]


def print_recommendations(user_id, recs):
    print(f"\n{'='*55}")
    print(f"  Top-{TOP_N} Recommendations for User {user_id}")
    print(f"{'='*55}")
    for r in recs:
        latency = f" | {r.get('latency_ms', 0):.1f}ms" if "latency_ms" in r else ""
        note    = f" [{r.get('note', '')}]" if r.get("note") else ""
        score   = r.get("predicted_rating") or r.get("avg_rating", 0)
        print(f"  {r['rank']}. {r['title']:<40} score={score:.4f}{latency}{note}")
    print(f"{'='*55}\n")


def run(test_user_ids=None):
    spark      = create_spark_session()
    model      = load_model()
    ratings_df, movies_df = load_data(spark)

    if test_user_ids is None:
        test_user_ids = [1, 42, 100, 9999]

    for uid in test_user_ids:
        recs = get_user_recommendations(
            spark, model, uid, ratings_df, movies_df
        )
        print_recommendations(uid, recs)

    spark.stop()


if __name__ == "__main__":
    run()
