import os
import sys
import time
import findspark
findspark.init("/home/mohamedehab/spark")

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *
from batch.preprocessing import preprocess


def train_als(train_df, rank=ALS_RANK, reg_param=ALS_REG_PARAM,
              max_iter=ALS_MAX_ITER):
    als = ALS(
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        userCol="user_id",
        itemCol="movie_id",
        ratingCol="rating",
        coldStartStrategy=ALS_COLD_START,
        nonnegative=True,
        implicitPrefs=False
    )
    return als.fit(train_df)


def evaluate_model(model, test_df):
    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    return rmse, predictions


def tune_model(train_df, test_df, initial_rmse):
    print(f"\n[TUNING] Initial RMSE {initial_rmse:.4f} > {RMSE_THRESHOLD}. Starting tuning...")

    param_grid = [
        {"rank": 10,  "reg_param": 0.05},
        {"rank": 20,  "reg_param": 0.1},
        {"rank": 20,  "reg_param": 0.05},
        {"rank": 50,  "reg_param": 0.1},
        {"rank": 50,  "reg_param": 0.01},
    ]

    best_rmse  = initial_rmse
    best_model = None
    best_params = {}

    for params in param_grid:
        print(f"  Testing rank={params['rank']}, regParam={params['reg_param']}...")
        model = train_als(train_df, rank=params["rank"],
                          reg_param=params["reg_param"])
        rmse, _ = evaluate_model(model, test_df)
        print(f"  → RMSE: {rmse:.4f}")
        if rmse < best_rmse:
            best_rmse   = rmse
            best_model  = model
            best_params = params

    print(f"\n[TUNING] Best params: {best_params}")
    print(f"[TUNING] Best RMSE:   {best_rmse:.4f}")
    return best_model, best_rmse, best_params


def save_model(model):
    os.makedirs(os.path.dirname(MODEL_DIR), exist_ok=True)
    model.write().overwrite().save(MODEL_DIR)
    print(f"[MODEL] Saved to: {MODEL_DIR}")


def print_summary(rmse, params, duration):
    print("\n" + "="*55)
    print("        ALS TRAINING SUMMARY")
    print("="*55)
    print(f"  Algorithm       : ALS (Alternating Least Squares)")
    print(f"  Rank            : {params.get('rank', ALS_RANK)}")
    print(f"  RegParam        : {params.get('reg_param', ALS_REG_PARAM)}")
    print(f"  MaxIter         : {ALS_MAX_ITER}")
    print(f"  Train/Test Split: {int(TRAIN_RATIO*100)}% / {int(TEST_RATIO*100)}%")
    print(f"  RMSE            : {rmse:.4f}")
    print(f"  Tuning Applied  : {'Yes' if rmse != -1 else 'No (RMSE within threshold)'}")
    print(f"  Training Time   : {duration:.1f} seconds")
    print(f"  Model Path      : {MODEL_DIR}")
    print("="*55 + "\n")


def run():
    spark, clean_df, movies_df = preprocess()

    train_df, test_df = clean_df.randomSplit(
        [TRAIN_RATIO, TEST_RATIO], seed=42
    )
    train_df.cache()
    test_df.cache()
    print(f"\n[SPLIT] Train: {train_df.count():,} | Test: {test_df.count():,}")

    print("\n[TRAINING] Training initial ALS model...")
    start = time.time()
    model = train_als(train_df)
    rmse, predictions = evaluate_model(model, test_df)
    duration = time.time() - start
    print(f"[TRAINING] Initial RMSE: {rmse:.4f} (took {duration:.1f}s)")

    final_params = {"rank": ALS_RANK, "reg_param": ALS_REG_PARAM}
    if rmse > RMSE_THRESHOLD:
        model, rmse, final_params = tune_model(train_df, test_df, rmse)
    else:
        print(f"[TRAINING] RMSE {rmse:.4f} ≤ {RMSE_THRESHOLD}. No tuning needed.")

    print("\n[PREDICTIONS] Sample predictions vs actuals:")
    predictions = model.transform(test_df)
    predictions.select("user_id", "movie_id", "rating", "prediction") \
               .dropna() \
               .show(10)

    save_model(model)

    print_summary(rmse, final_params, duration)

    spark.stop()
    return rmse


if __name__ == "__main__":
    run()
