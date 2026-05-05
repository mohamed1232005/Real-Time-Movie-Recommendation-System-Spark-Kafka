import os
import sys
import findspark
findspark.init("/home/mohamedehab/spark")

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *

OUTPUT_PATH = os.path.join(BASE_DIR, "models/precomputed_recommendations")

spark = SparkSession.builder \
    .appName("PrecomputeRecommendations") \
    .master(SPARK_MASTER) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

model = ALSModel.load(MODEL_DIR)

print("[PRECOMPUTE] Generating Top-5 recommendations for all users...")
recs = model.recommendForAllUsers(TOP_N)

recs.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"[PRECOMPUTE] Saved to: {OUTPUT_PATH}")
spark.stop()
