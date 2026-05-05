import os

BASE_DIR        = os.path.expanduser("~/movie-recommendation-system")
DATA_DIR        = os.path.join(BASE_DIR, "data/ml-1m")
MODEL_DIR       = os.path.join(BASE_DIR, "models/als_model")
CHECKPOINT_DIR  = os.path.join(BASE_DIR, "checkpoints")
OUTPUT_RECS     = os.path.join(BASE_DIR, "outputs/recommendations")
OUTPUT_ALERTS   = os.path.join(BASE_DIR, "outputs/alerts")

RATINGS_FILE    = os.path.join(DATA_DIR, "ratings.dat")
MOVIES_FILE     = os.path.join(DATA_DIR, "movies.dat")
USERS_FILE      = os.path.join(DATA_DIR, "users.dat")

SPARK_HOME      = "/home/mohamedehab/spark"
SPARK_APP_NAME  = "MovieRecommendationSystem"
SPARK_MASTER    = "local[*]"

KAFKA_BROKER        = "localhost:9092"
KAFKA_TOPIC         = "movie-ratings"
KAFKA_NUM_PARTITIONS = 2

ALS_RANK            = 10
ALS_MAX_ITER        = 10
ALS_REG_PARAM       = 0.1
ALS_COLD_START      = "drop"
TRAIN_RATIO         = 0.8
TEST_RATIO          = 0.2
RMSE_THRESHOLD      = 1.5

WINDOW_DURATION     = "30 seconds"
SLIDE_DURATION      = "10 seconds"
WATERMARK_DELAY     = "15 seconds"

ALERT_RATING_THRESHOLD      = 4.5
ALERT_INTERACTION_THRESHOLD = 10
TRENDING_SCORE_THRESHOLD    = 30

PRODUCER_SLEEP_INTERVAL = 0.2
PRODUCER_BATCH_SIZE     = 1

TOP_N = 5

DASHBOARD_REFRESH_SECONDS = 5
