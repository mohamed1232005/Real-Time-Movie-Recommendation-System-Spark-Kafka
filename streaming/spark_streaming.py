import os
import sys
import time
import json
import findspark
findspark.init("/home/mohamedehab/spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *
from streaming.alert_system import evaluate_window_alerts

EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True),
])

PRECOMPUTED_RECS_PATH = os.path.join(BASE_DIR, "models/precomputed_recommendations")


def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME + "_Streaming_Fast") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_movies_lookup(spark):
    schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("genres", StringType(), False),
    ])
    movies_df = spark.read.option("sep", "::").schema(schema).csv(MOVIES_FILE)
    return {row["movie_id"]: row["title"] for row in movies_df.collect()}


def read_kafka_stream(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_stream(raw_df):
    parsed = raw_df.select(
        F.from_json(F.col("value").cast(StringType()), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    clean = parsed.filter(
        F.col("user_id").isNotNull() &
        F.col("item_id").isNotNull() &
        F.col("rating").isNotNull() &
        F.col("timestamp").isNotNull()
    )

    clean = clean.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    return clean


def build_item_window_agg(df):
    return df.withWatermark("event_time", WATERMARK_DELAY) \
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("item_id")
        ) \
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("interaction_count")
        ) \
        .withColumn("avg_rating", F.round("avg_rating", 4)) \
        .withColumn("trending_score", F.round(F.col("avg_rating") * F.col("interaction_count"), 2))


def build_user_window_agg(df):
    return df.withWatermark("event_time", WATERMARK_DELAY) \
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("user_id")
        ) \
        .agg(
            F.count("*").alias("user_interaction_count"),
            F.avg("rating").alias("user_avg_rating")
        ) \
        .withColumn("user_avg_rating", F.round("user_avg_rating", 4))


def process_item_batch(batch_df, batch_id, movies_lookup):
    if batch_df.isEmpty():
        return

    rows = batch_df.orderBy(F.col("trending_score").desc()).limit(10).collect()

    print(f"\n{'-'*70}")
    print(f"WINDOW BATCH {batch_id} — Top Trending Movies")
    print(f"{'-'*70}")
    print(f"{'item_id':>8} | {'title':<35} | {'avg':>6} | {'count':>6} | {'score':>8}")

    window_data = []
    for row in rows:
        title = movies_lookup.get(row["item_id"], "Unknown")[:35]
        print(f"{row['item_id']:>8} | {title:<35} | {row['avg_rating']:>6.2f} | {row['interaction_count']:>6} | {row['trending_score']:>8.2f}")
        window_data.append({
            "item_id": row["item_id"],
            "avg_rating": row["avg_rating"],
            "interaction_count": row["interaction_count"],
            "trending_score": row["trending_score"],
        })

    alerts = evaluate_window_alerts(window_data, movies_lookup)
    if alerts > 0:
        print(f"ALERTS FIRED: {alerts}")


def process_user_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n[USER ACTIVITY] Batch {batch_id}")
    batch_df.orderBy(F.col("user_interaction_count").desc()).show(5, truncate=False)


def generate_fast_recommendations(batch_df, batch_id, precomputed_recs, movies_lookup):
    if batch_df.isEmpty():
        return

    start_time = time.time()

    users_df = batch_df.select("user_id").distinct().limit(10)
    joined = users_df.join(precomputed_recs, on="user_id", how="left")
    rows = joined.collect()

    latency_ms = (time.time() - start_time) * 1000

    print(f"\n{'='*70}")
    print(f"BATCH {batch_id} — Fast Top-{TOP_N} Recommendations")
    print(f"Users in batch: {len(rows)} | Latency: {latency_ms:.1f}ms")
    print(f"{'='*70}")

    os.makedirs(OUTPUT_RECS, exist_ok=True)
    out_path = os.path.join(OUTPUT_RECS, f"batch_{batch_id}.json")

    with open(out_path, "w") as f:
        for row in rows[:5]:
            uid = row["user_id"]
            recs = row["recommendations"]

            if recs is None:
                print(f"User {uid} → Cold-start fallback needed")
                continue

            titles = []
            json_recs = []

            for r in recs:
                mid = r["movie_id"]
                title = movies_lookup.get(mid, "Movie " + str(mid))
                score = float(r["rating"])
                titles.append(f"{title} ({score:.2f})")
                json_recs.append({
                    "movie_id": mid,
                    "title": title,
                    "predicted_rating": round(score, 4)
                })

            print(f"User {uid} → " + " | ".join(titles))

            json.dump({
                "user_id": uid,
                "recommendations": json_recs,
                "latency_ms": round(latency_ms, 2),
                "source": "precomputed ALS recommendations"
            }, f)
            f.write("\n")

    if latency_ms < 5000:
        print(f"✅ BONUS ACHIEVED: Latency {latency_ms:.1f}ms < 5000ms")
    else:
        print(f"⚠️ Latency {latency_ms:.1f}ms exceeded 5000ms")


def run():
    print("\n" + "="*70)
    print("SPARK STRUCTURED STREAMING — FAST RECOMMENDATION MODE")
    print("="*70)

    spark = create_spark_session()
    movies_lookup = load_movies_lookup(spark)

    print(f"[STREAMING] Loading precomputed recommendations from: {PRECOMPUTED_RECS_PATH}")
    precomputed_recs = spark.read.parquet(PRECOMPUTED_RECS_PATH).cache()
    print(f"[STREAMING] Precomputed recommendations loaded: {precomputed_recs.count()} users")

    raw_df = read_kafka_stream(spark)
    parsed_df = parse_stream(raw_df)

    item_agg = build_item_window_agg(parsed_df)
    user_agg = build_user_window_agg(parsed_df)

    item_query = item_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, bid: process_item_batch(df, bid, movies_lookup)) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "item_agg_fast")) \
        .start()

    user_query = user_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, bid: process_user_batch(df, bid)) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "user_agg_fast")) \
        .start()

    rec_query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, bid: generate_fast_recommendations(df, bid, precomputed_recs, movies_lookup)) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "recs_fast")) \
        .start()

    print("\n[STREAMING] All queries started. Waiting for data...")
    print("[STREAMING] Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[STREAMING] Stopping...")
        item_query.stop()
        user_query.stop()
        rec_query.stop()
        spark.stop()
        print("[STREAMING] Shutdown complete.")


if __name__ == "__main__":
    run()
