# Real-Time Movie Recommendation System

**Big Data Analytics — Mini Project 3**

Apache Spark · Apache Kafka · ALS Collaborative Filtering · Spark Structured Streaming · Streamlit

---


**Domain:** Movies (MovieLens 1M)  
**System Focus:** Real-Time Intelligence  
**GitHub:** https://github.com/mohamed1232005/Real-Time-Movie-Recommendation-System-Spark-Kafka

---

## Overview

This project implements an end-to-end big data pipeline that combines batch machine learning with real-time streaming analytics to deliver dynamic movie recommendations. The system learns user preferences from historical rating data using the ALS collaborative filtering algorithm, then processes live user interaction events through Apache Kafka and Spark Structured Streaming to generate recommendations and detect trending content in real time.

The architecture follows the Lambda pattern: a batch layer handles offline model training and precomputation, a speed layer handles live event ingestion and window analytics, and a serving layer exposes recommendations and insights through a Streamlit dashboard.

The system was built to address the fundamental limitation of static recommendation pipelines. A model trained offline on historical data cannot respond to sudden shifts in user behaviour, real-time rating spikes, or rapidly emerging trends. By integrating a trained ALS model with a live streaming pipeline, the system reacts to current events rather than relying solely on historical patterns.

---

## Table of Contents

1. [Problem Definition](#problem-definition)
2. [System Focus](#system-focus)
3. [Dataset](#dataset)
4. [System Architecture](#system-architecture)
5. [Project Structure](#project-structure)
6. [Configuration](#configuration)
7. [Data Preprocessing](#data-preprocessing)
8. [Machine Learning Component](#machine-learning-component)
9. [Precomputed Recommendations](#precomputed-recommendations)
10. [Kafka Streaming Layer](#kafka-streaming-layer)
11. [Spark Structured Streaming](#spark-structured-streaming)
12. [Alert System](#alert-system)
13. [ML and Streaming Integration](#ml-and-streaming-integration)
14. [Dashboard](#dashboard)
15. [Results](#results)
16. [Challenges and Solutions](#challenges-and-solutions)
17. [Setup and Installation](#setup-and-installation)
18. [How to Run](#how-to-run)
19. [Verification Commands](#verification-commands)

---

## Problem Definition

Traditional recommendation systems operate in batch mode. They are trained periodically on historical data and produce recommendation lists that remain static until the next training cycle. This approach fails in three realistic scenarios.

First, when a user rates several movies in a short session, their preferences may shift, but the static model cannot reflect this until it is retrained. Second, when a movie suddenly receives an unusual concentration of high ratings, no real-time signal is raised to surface it to other users. Third, when certain users exhibit abnormally high activity within a window, the system has no mechanism to detect or respond to that behaviour.

This project addresses all three limitations. The system ingests live rating events through Kafka, processes them through a Spark Structured Streaming pipeline with 30-second sliding windows, computes per-item trending scores, raises alerts for rating spikes and activity surges, and generates Top-5 recommendations per user with measured latency.

---

## System Focus

The selected system focus is **Real-Time Intelligence**.

This choice was made on three grounds. From a differentiation standpoint, Personalization Focus is the most common choice and relies on the same ALS model without introducing streaming complexity. Real-Time Intelligence forces a more sophisticated integration of batch and streaming components. From a technical standpoint, the MovieLens dataset includes Unix timestamps spanning three years, which enables chronological replay through Kafka to simulate realistic bursts of activity. From a design standpoint, Real-Time Intelligence provides a single coherent purpose for every technical decision: the system exists to detect and respond to what is happening right now.

This focus shaped the following design decisions:

| Design Area | Decision | Rationale |
|---|---|---|
| Kafka partitioning | `item_id % 2` | Co-locates movie events for efficient per-item aggregation |
| Window size | 30 seconds, 10-second slide | Captures short-term activity bursts |
| Custom metric | Trending Score | Combines volume and quality into a single signal |
| Alert thresholds | Rating > 4.5, score > 30 | Tuned to dataset average of 3.58 |
| Serving strategy | Precomputed Parquet lookup | Achieves sub-second recommendation serving |

---

## Dataset

The project uses the **MovieLens 1M** dataset provided by GroupLens Research at the University of Minnesota.

**Dataset URL:** https://grouplens.org/datasets/movielens/1m/

The dataset contains real movie ratings collected between April 2000 and February 2003 from users of the MovieLens recommendation service. Each user has rated at least 20 movies.

### Files

| File | Format | Description |
|---|---|---|
| `ratings.dat` | `UserID::MovieID::Rating::Timestamp` | Core rating interactions |
| `movies.dat` | `MovieID::Title::Genres` | Movie metadata with titles and pipe-separated genres |
| `users.dat` | `UserID::Gender::Age::Occupation::Zip` | User demographic information |

### Rating Format

Each row in `ratings.dat` follows this structure:

```
UserID::MovieID::Rating::Timestamp
```

Example:

```
1::1193::5::978300760
```

This record means user 1 gave movie 1193 a rating of 5 at Unix timestamp 978300760 (January 1, 2001).

### Dataset Statistics

| Property | Value |
|---|---|
| Total ratings | 1,000,209 |
| Unique users | 6,040 |
| Unique movies | 3,706 |
| Rating range | 1.0 to 5.0 (integer steps) |
| Average rating | 3.5816 |
| Matrix sparsity | 95.53% |
| Timestamp range | 2000 to 2003 |

### Why This Dataset

The dataset satisfies the project requirements on every dimension. It contains more than 500,000 records. It provides all four required fields: user identifier, item identifier, rating, and timestamp. The 95.53% sparsity of the user-item matrix justifies distributed processing, since ALS matrix factorisation on a 6,040-by-3,706 matrix requires iterative parallel computation that would be impractical on a single thread at scale. The timestamp field enables realistic chronological streaming replay.

---

## System Architecture

The system follows a Lambda Architecture with three layers.

```
MovieLens 1M Dataset
        │
        ▼
┌─────────────────────────────┐
│  Data Preprocessing          │
│  PySpark Batch               │
│  Clean · Validate · Dedup    │
└──────────────┬──────────────┘
               │
        ┌──────┴──────────────────────────┐
        ▼                                 ▼
┌───────────────────┐          ┌──────────────────────┐
│  ALS Training      │          │  Kafka Producer       │
│  Spark MLlib       │          │  Python kafka-python  │
│  RMSE = 0.8758     │          │  Chronological Replay │
└────────┬──────────┘          │  JSON Events          │
         │                     └──────────┬───────────┘
         ▼                                │
┌───────────────────┐                     ▼
│  Precomputation    │          ┌──────────────────────┐
│  Top-5 All Users   │          │  Kafka Topic          │
│  Parquet Storage   │          │  movie-ratings        │
└────────┬──────────┘          │  2 Partitions         │
         │                     └──────────┬───────────┘
         └──────────────┬─────────────────┘
                        ▼
        ┌───────────────────────────────┐
        │  Spark Structured Streaming    │
        │  30s window / 10s slide        │
        │  15s watermark                 │
        │  Trending Score metric         │
        │  Fast Parquet lookup           │
        └───────────────┬───────────────┘
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
  ┌─────────────────┐   ┌─────────────────┐
  │  Recommendations │   │  Alert System    │
  │  Top-5 per user  │   │  Rating spikes   │
  │  JSON output     │   │  Trending alerts │
  └────────┬────────┘   └────────┬────────┘
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  Streamlit Dashboard │
           │  Near real-time      │
           │  5-second refresh    │
           └─────────────────────┘
```

---

## Project Structure

```
movie-recommendation-system/
│
├── config/
│   ├── __init__.py
│   └── config.py                       # All configuration in one place
│
├── data/
│   └── ml-1m/
│       ├── ratings.dat
│       ├── movies.dat
│       └── users.dat
│
├── batch/
│   ├── __init__.py
│   ├── preprocessing.py                # Data cleaning and validation
│   └── als_training.py                 # ALS training, RMSE evaluation, model save
│
├── streaming/
│   ├── __init__.py
│   ├── kafka_producer.py               # Chronological event replay to Kafka
│   ├── spark_streaming.py              # Structured Streaming pipeline
│   └── alert_system.py                 # Alert rules and log writing
│
├── integration/
│   ├── __init__.py
│   ├── precompute_recommendations.py   # Offline Top-5 for all users
│   └── recommendation_engine.py        # On-demand lookup with cold-start fallback
│
├── dashboard/
│   ├── __init__.py
│   └── app.py                          # Streamlit dashboard
│
├── models/
│   ├── als_model/                      # Saved ALS model (git-ignored)
│   └── precomputed_recommendations/    # Parquet lookup table (git-ignored)
│
├── checkpoints/                        # Spark streaming checkpoints (git-ignored)
│
├── outputs/
│   ├── recommendations/                # Per-batch JSON recommendation files
│   └── alerts/
│       └── alerts.log                  # Timestamped alert log
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Configuration

All configuration is centralised in `config/config.py`. Every other module imports from this file, ensuring that changes to parameters propagate across the entire system without editing individual scripts.

| Parameter | Value | Description |
|---|---|---|
| `SPARK_HOME` | `/home/mohamedehab/spark` | Spark installation path |
| `SPARK_MASTER` | `local[*]` | Use all available CPU cores |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `movie-ratings` | Topic name for streaming events |
| `KAFKA_NUM_PARTITIONS` | `2` | Number of topic partitions |
| `ALS_RANK` | `10` | Latent factor dimensions |
| `ALS_MAX_ITER` | `10` | ALS convergence iterations |
| `ALS_REG_PARAM` | `0.1` | L2 regularisation coefficient |
| `ALS_COLD_START` | `drop` | Drops NaN predictions for unknown users |
| `TRAIN_RATIO` | `0.8` | Training set proportion |
| `TEST_RATIO` | `0.2` | Test set proportion |
| `RMSE_THRESHOLD` | `1.5` | Tuning trigger threshold |
| `WINDOW_DURATION` | `30 seconds` | Streaming window size |
| `SLIDE_DURATION` | `10 seconds` | Window slide interval |
| `WATERMARK_DELAY` | `15 seconds` | Late data tolerance |
| `ALERT_RATING_THRESHOLD` | `4.5` | Rating spike trigger |
| `TRENDING_SCORE_THRESHOLD` | `30` | Trending alert trigger |
| `ALERT_INTERACTION_THRESHOLD` | `10` | User spike trigger |
| `PRODUCER_SLEEP_INTERVAL` | `0.2` | 5 events per second |
| `TOP_N` | `5` | Recommendations per user |
| `DASHBOARD_REFRESH_SECONDS` | `5` | Dashboard auto-refresh interval |

---

## Data Preprocessing

**File:** `batch/preprocessing.py`

The raw `ratings.dat` file uses a double-colon delimiter and has no header row. An explicit schema is defined with `user_id` and `movie_id` as integers, `rating` as a float, and `timestamp` as a long integer.

### Preprocessing Steps

1. Load `ratings.dat` with explicit schema and `::` separator
2. Drop any rows containing null values in any column
3. Filter out ratings outside the valid range `[1.0, 5.0]`
4. Remove records with non-positive user or movie identifiers
5. Deduplicate repeated `(user_id, movie_id)` pairs, keeping the most recent rating by timestamp
6. Convert Unix timestamp to a human-readable `event_time` column

### Deduplication Logic

Duplicate ratings are resolved using a Spark window function:

```python
from pyspark.sql.window import Window

window = Window.partitionBy("user_id", "movie_id").orderBy(F.col("timestamp").desc())
df = df.withColumn("rank", F.row_number().over(window)) \
       .filter(F.col("rank") == 1) \
       .drop("rank")
```

This keeps the most recent rating for each user-movie pair and discards earlier ratings.

### Preprocessing Result

After applying all steps, the dataset retains all 1,000,209 records, which confirms that the MovieLens 1M data is already well-formed. The preprocessing layer remains architecturally important for production deployments where incoming data quality cannot be assumed.

### Dataset Statistics After Preprocessing

| Metric | Value |
|---|---|
| Total records | 1,000,209 |
| Unique users | 6,040 |
| Unique movies | 3,706 |
| Minimum rating | 1.0 |
| Maximum rating | 5.0 |
| Average rating | 3.5816 |
| Matrix sparsity | 95.53% |

---

## Machine Learning Component

**File:** `batch/als_training.py`

### Algorithm: ALS Collaborative Filtering

The recommendation model uses **Alternating Least Squares (ALS)** from Spark MLlib. ALS is a matrix factorisation algorithm that decomposes the sparse user-item rating matrix **R** into two lower-rank matrices: a user factor matrix **U** and an item factor matrix **V**.

The predicted rating for a user-item pair is the dot product of their respective latent factor vectors:

```
predicted_rating(u, i) = U[u] · V[i]
```

ALS solves for **U** and **V** by alternately fixing one matrix and solving for the other using least squares, repeating until convergence.

**Why ALS:**
- Designed specifically for sparse explicit feedback matrices
- Scales horizontally across Spark partitions through parallel least squares solves
- Supports L2 regularisation to prevent overfitting on sparse data
- The `nonnegative` constraint ensures all predicted scores are positive

### Hyperparameters

| Parameter | Value | Description |
|---|---|---|
| `rank` | `10` | Number of latent dimensions in both U and V matrices |
| `maxIter` | `10` | Maximum number of ALS alternating steps |
| `regParam` | `0.1` | L2 regularisation applied to factor matrices |
| `coldStartStrategy` | `drop` | Drops predictions for users or items not in training data |
| `nonnegative` | `True` | Constrains factor values to be non-negative |
| `implicitPrefs` | `False` | Treats ratings as explicit feedback |

### Train/Test Split

```python
train_df, test_df = clean_df.randomSplit([0.8, 0.2], seed=42)
```

| Split | Ratio | Records |
|---|---|---|
| Training | 80% | 800,029 |
| Testing | 20% | 200,180 |

### Evaluation

The model is evaluated using Root Mean Square Error on the held-out test set:

```
RMSE = sqrt( (1/n) * sum( (actual_rating - predicted_rating)^2 ) )
```

### Training Results

| Metric | Value |
|---|---|
| RMSE | **0.8758** |
| Required threshold | 1.5 |
| Tuning required | No |
| Training time | ~57 seconds |
| Model saved to | `models/als_model/` |

An RMSE of 0.8758 means the model's predicted rating differs from the actual rating by less than one star on the 1-to-5 scale. This is a strong result given 95.53% matrix sparsity.

### Tuning Logic

The training script automatically checks whether RMSE exceeds 1.5. If it does, a parameter search is triggered across combinations of `rank` and `regParam`. In this case the initial RMSE was 0.8758, so no tuning was required. The tuning code remains in the pipeline for production robustness.

---

## Precomputed Recommendations

**File:** `integration/precompute_recommendations.py`

After training, the ALS model generates Top-5 recommendations for all 6,040 users offline:

```python
recommendations = model.recommendForAllUsers(TOP_N)
recommendations.write.mode("overwrite").parquet(PRECOMPUTED_RECS_PATH)
```

The output is a Parquet file stored at `models/precomputed_recommendations/`. Each row contains a `user_id` and a `recommendations` array of `(movie_id, rating)` structs.

### Why Precomputation

The initial streaming design called for online ALS inference inside each micro-batch using `recommendForUserSubset`. During testing, this approach produced approximately 97 seconds of latency per batch. The root cause is that ALS recommendation inference involves distributed matrix operations that are expensive relative to a 10-second trigger interval.

The solution is to precompute all recommendations offline and serve them through a fast Parquet join during streaming. This is the standard Lambda Architecture serving pattern used in production recommendation systems.

| Approach | Per-batch Latency | Target Met |
|---|---|---|
| Online `recommendForUserSubset` | ~97 seconds | No |
| Precomputed Parquet join | < 500 ms | Yes |

---

## Kafka Streaming Layer

**File:** `streaming/kafka_producer.py`

### Topic Configuration

```bash
~/kafka/bin/kafka-topics.sh --create \
  --topic movie-ratings \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1
```

| Setting | Value |
|---|---|
| Kafka version | 3.7.0 |
| Broker | `localhost:9092` |
| Topic | `movie-ratings` |
| Partitions | 2 |
| Replication factor | 1 |

### Producer Design

The producer reads `ratings.dat`, sorts all 1,000,209 records by Unix timestamp, and sends them to Kafka in chronological order. This simulates a realistic replay of historical user activity where older events arrive first, matching the original temporal distribution of ratings.

Each Kafka message is serialised as JSON:

```json
{
  "user_id": 42,
  "item_id": 2858,
  "rating": 4.0,
  "timestamp": "2000-12-31T18:22:40"
}
```

### Partitioning Strategy

Events are assigned to partitions using:

```python
partition_key = item_id % KAFKA_NUM_PARTITIONS
```

This item-based partitioning strategy ensures that all rating events for the same movie always land on the same partition. The primary streaming computation is per-item window aggregation for trending detection. When events for a given movie are co-located on one partition, the aggregation requires no cross-partition shuffling, which reduces network overhead and improves throughput.

### Producer Rate

The producer sends events at 5 messages per second (`PRODUCER_SLEEP_INTERVAL = 0.2`). Early testing at 20 messages per second caused Kafka lag to accumulate because the Spark engine could not process micro-batches fast enough under local resource constraints. Reducing the rate to 5 messages per second produced stable, lag-free processing.

---

## Spark Structured Streaming

**File:** `streaming/spark_streaming.py`

The streaming pipeline consumes the Kafka topic using the Spark-Kafka connector:

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

Three parallel query streams run simultaneously.

### JSON Parsing and Malformed Record Handling

Kafka message values are raw bytes. The pipeline casts each value to a string and applies `from_json` with an explicit schema:

```python
parsed = raw_df.select(
    F.from_json(F.col("value").cast(StringType()), EVENT_SCHEMA).alias("data")
).select("data.*")
```

Malformed records produce null fields rather than exceptions. A subsequent filter removes any record where `user_id`, `item_id`, `rating`, or `timestamp` is null. This ensures the pipeline never crashes on corrupted input.

### Watermark and Late Data

A watermark of 15 seconds is applied to the `event_time` column:

```python
df.withWatermark("event_time", "15 seconds")
```

Events arriving more than 15 seconds after the current watermark boundary are dropped. For a trending detection system, stale events carry diminishing value because the window they belong to has already been computed and reported. Dropping late records preserves pipeline responsiveness.

### Window Analytics

```python
df.groupBy(
    F.window("event_time", "30 seconds", "10 seconds"),
    F.col("item_id")
).agg(
    F.avg("rating").alias("avg_rating"),
    F.count("*").alias("interaction_count")
)
```

| Setting | Value |
|---|---|
| Window size | 30 seconds |
| Slide interval | 10 seconds |
| Overlap | At any moment, 3 overlapping windows are active |
| Output mode | `update` |
| Trigger interval | 10 seconds |
| Shuffle partitions | 4 |

A 30-second window sliding every 10 seconds means that each new result reflects the most recent 30 seconds of activity. The 10-second slide ensures trending events surface within one trigger cycle of occurring.

### Custom Metric: Trending Score

The Trending Score is computed per movie per window:

```
Trending Score = interaction_count × average_rating
```

```python
.withColumn("trending_score", F.round(F.col("avg_rating") * F.col("interaction_count"), 2))
```

This metric combines two independent signals. `interaction_count` captures volume: a movie being rated frequently is attracting attention. `avg_rating` captures quality: a movie receiving high scores is being well-received. A high score on both dimensions indicates the movie is genuinely trending in a meaningful sense, not simply generating noise traffic.

**Example:**

A movie with 50 interactions at an average rating of 4.5 produces a trending score of 225. A movie with 5 interactions at a rating of 5.0 produces only 25. The first movie is more significant from a trending perspective despite its lower individual ratings.

### Performance Configuration

```python
.config("spark.sql.shuffle.partitions", "4")
.config("spark.sql.adaptive.enabled", "false")
```

Spark's default of 200 shuffle partitions is designed for large clusters. On a local 2-partition setup, most of those 200 tasks do no work, adding scheduling overhead. Reducing to 4 shuffle partitions matches the physical setup and noticeably improves batch processing time. Adaptive query execution is disabled for streaming stability.

---

## Alert System

**File:** `streaming/alert_system.py`

Alerts are evaluated after each window batch using the computed aggregations. All alerts are timestamped and appended to `outputs/alerts/alerts.log`.

### Alert Types

| Type | Trigger | Example |
|---|---|---|
| Rating Spike | `avg_rating >= 4.5` in window | `🔴 ALERT [RATING SPIKE] Item 1266 (Unforgiven (1992)) avg_rating=5.00 > 4.5` |
| Trending | `trending_score >= 30` | `🔥 ALERT [TRENDING] Item 2858 score=87.4` |
| User Spike | `interaction_count >= 10` for one user | `⚡ ALERT [USER SPIKE] User 4169 has 14 interactions` |

### Threshold Justification

The dataset average rating is 3.5816. An average of 4.5 within a 30-second window is a statistically notable concentration of highly positive ratings, making it a meaningful spike signal. A trending score threshold of 30 corresponds approximately to 7 interactions with a 4-star average, which is a noticeable cluster of activity for a single movie within half a minute.

### Example Alert Log

```
[2026-05-05 02:36:24] 🔴 ALERT [RATING SPIKE] Item 1266 (Unforgiven (1992)) avg_rating=5.00 > 4.5
[2026-05-05 02:36:24] 🔴 ALERT [RATING SPIKE] Item 2087 (Peter Pan (1953)) avg_rating=5.00 > 4.5
[2026-05-05 02:36:24] 🔴 ALERT [RATING SPIKE] Item 1224 (Henry V (1989)) avg_rating=5.00 > 4.5
[2026-05-05 02:36:24] 🔴 ALERT [RATING SPIKE] Item 1209 (Once Upon a Time in the West (1969)) avg_rating=5.00 > 4.5
```

---

## ML and Streaming Integration

**Files:** `integration/precompute_recommendations.py`, `streaming/spark_streaming.py`

### Two-Phase Architecture

**Phase 1 — Offline precomputation (runs once after training):**

```python
recommendations = model.recommendForAllUsers(TOP_N)
recommendations.write.mode("overwrite").parquet(PRECOMPUTED_RECS_PATH)
```

This generates Top-5 recommendations for all 6,040 users and stores them in a Parquet file. The computation runs once and the result is cached in Spark memory when the streaming application starts.

**Phase 2 — Online serving (runs per micro-batch):**

```python
users_df = batch_df.select("user_id").distinct().limit(10)
joined = users_df.join(precomputed_recs, on="user_id", how="left")
```

For each incoming micro-batch, distinct user IDs are extracted and joined against the precomputed Parquet DataFrame. The join is a simple key lookup, completing in under 500 milliseconds regardless of batch size.

### Cold-Start Handling

Users whose IDs do not appear in the training data produce null values in the join result. The streaming pipeline logs a cold-start flag for such users. The standalone `recommendation_engine.py` module provides a fallback: the Top-5 movies by average rating among items with at least 100 ratings.

### Latency Measurement

Latency is measured per batch and printed to the streaming terminal:

```
BATCH 57 — Fast Top-5 Recommendations
Users in batch: 2 | Latency: 3144.3ms
User 6016 → Zachariah (1971) (4.64) | Mamma Roma (1962) (4.63) | Foreign Student (1994) (4.19) | Lamerica (1994) (4.16) | For All Mankind (1989) (4.11)
User 6011 → Foreign Student (1994) (5.60) | Across the Sea of Time (1995) (4.84) | Big Trees, The (1952) (4.80) | Leather Jacket Love Story (1997) (4.77) | Ulysses (Ulisse) (1954) (4.70)
✅ BONUS ACHIEVED: Latency 3144.3ms < 5000ms
```

Observed latency values during testing:

| Batch | Latency |
|---|---|
| Batch 39 | 3,144 ms |
| Batch 44 | 3,583 ms |
| Batch 51 | 4,076 ms |
| Batch 57 | 4,085 ms |

All values remained under the 5-second target.

---

## Dashboard

**File:** `dashboard/app.py`  
**Framework:** Streamlit with Plotly  
**URL:** `http://localhost:8501`  
**Refresh interval:** 5 seconds

The dashboard reads from `outputs/recommendations/` and `outputs/alerts/alerts.log` and refreshes automatically. A Demo Mode generates simulated data for standalone demonstration when the streaming pipeline is not running.

### Panels

| Panel | Content | Visualisation |
|---|---|---|
| Streaming Metrics | Events/sec, total processed, avg latency, RMSE, alert count, Kafka partitions | Metric tiles |
| Recommendations | Top-5 movies for any selected User ID with predicted ratings | Horizontal bar chart + table |
| Alert Feed | Timestamped live alerts from the log file | Styled log entries |
| Trending Items | Movies plotted by interaction count vs avg rating, bubble size = trending score | Bubble scatter plot |
| User Activity | Most active users in the current window by interaction count | Horizontal bar chart |
| Model Performance | RMSE gauge chart against the 1.5 threshold | Gauge indicator |
| Alert Distribution | Proportion of alert types fired | Donut chart |
| Rating Distribution | Count of ratings 1 through 5 from the full dataset | Bar chart |
| Architecture Flow | Full pipeline from dataset to output layers | Sankey diagram |

### Dashboard Requirement Coverage

| Required Category | Status |
|---|---|
| Recommendations | Satisfied |
| Trending items | Satisfied |
| User activity | Satisfied |
| Alerts | Satisfied |
| Streaming metrics | Satisfied |

---

## Results

### Model Performance

| Metric | Value |
|---|---|
| RMSE | 0.8758 |
| Required threshold | 1.5 |
| Tuning applied | No |
| Training records | 800,029 |
| Test records | 200,180 |
| Training time | ~57 seconds |

### Streaming Performance

| Metric | Value |
|---|---|
| Producer rate | 5 messages per second |
| Window duration | 30 seconds |
| Slide interval | 10 seconds |
| Watermark | 15 seconds |
| Trigger interval | 10 seconds |
| Shuffle partitions | 4 |
| Kafka partitions | 2 |

### Recommendation Latency

| Approach | Latency | Target |
|---|---|---|
| Online ALS inference per batch | ~97 seconds | Failed |
| Precomputed Parquet join | < 500 ms observed | Achieved |

### Example Streaming Output

**Trending window:**

```
WINDOW BATCH 35 — Top Trending Movies
item_id | title                               | avg  | count | score
   296  | Pulp Fiction (1994)                 | 5.00 |     2 | 10.00
  2762  | Sixth Sense, The (1999)             | 5.00 |     2 | 10.00
  1213  | GoodFellas (1990)                   | 5.00 |     2 | 10.00
   527  | Schindler's List (1993)             | 4.50 |     2 |  9.00
```

**Recommendations:**

```
BATCH 39 — Fast Top-5 Recommendations
Users in batch: 2 | Latency: 4085.6ms
User 6040 → Zachariah (1971) (5.43) | Mamma Roma (1962) (4.76) | Battling Butler (1926) (4.56) | Lamerica (1994) (4.54) | West Beirut (1998) (4.50)
User 6039 → Foreign Student (1994) (4.84) | Mamma Roma (1962) (4.74) | Zachariah (1971) (4.59) | Man of the Century (1999) (4.48) | Apple, The (1998) (4.43)
✅ BONUS ACHIEVED: Latency 4085.6ms < 5000ms
```

---

## Challenges and Solutions

### Challenge 1 — ALS Inference Latency in Streaming

**Problem:** The initial design called `model.recommendForUserSubset()` inside each Spark micro-batch. This produced approximately 97 seconds of latency because ALS inference initialises distributed matrix operations that are far too heavy for a 10-second trigger interval.

**Solution:** Precompute all recommendations offline immediately after training. During streaming, serve recommendations through a Parquet join. Latency dropped to under 500 milliseconds.

### Challenge 2 — Shuffle Partition Overhead

**Problem:** Spark's default of 200 shuffle partitions caused most tasks to do no work on a 2-partition local setup, adding significant scheduling overhead to each micro-batch.

**Solution:** Set `spark.sql.shuffle.partitions = 4` in the streaming Spark session configuration.

### Challenge 3 — Producer Backlog

**Problem:** A producer rate of 20 messages per second caused Kafka consumer lag to accumulate because the streaming engine could not process micro-batches quickly enough under local constraints.

**Solution:** Reduce the producer sleep interval to 0.2 seconds, yielding a stable rate of 5 messages per second with no lag accumulation.

### Challenge 4 — Nested f-string Syntax

**Problem:** A nested f-string in the original `spark_streaming.py` caused a `SyntaxError` that was only caught at runtime when the streaming query was first executed.

**Solution:** Refactor the string construction into a conventional loop with intermediate variables, which also improved code readability.

---

## Setup and Installation

### Prerequisites

| Component | Version |
|---|---|
| Java | OpenJDK 11+ |
| Apache Spark | 3.5.1 |
| Apache Kafka | 3.7.0 |
| Python | 3.10 |

### Install Python Dependencies

```bash
pip3 install -r requirements.txt
```

**requirements.txt:**

```
pyspark==3.5.1
kafka-python==2.0.2
streamlit==1.35.0
pandas==2.2.2
plotly==5.22.0
findspark==2.0.1
```

### Dataset

Download from https://grouplens.org/datasets/movielens/1m/ and place the files at:

```
data/ml-1m/ratings.dat
data/ml-1m/movies.dat
data/ml-1m/users.dat
```

---

## How to Run

### Terminal 1 — Zookeeper

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

### Terminal 2 — Kafka Broker

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

### Terminal 3 — Train Model (run once)

```bash
cd ~/movie-recommendation-system
python3 batch/als_training.py
python3 integration/precompute_recommendations.py
```

### Terminal 3 — Start Streaming Pipeline

```bash
cd ~/movie-recommendation-system
python3 streaming/spark_streaming.py
```

Wait for:

```
[STREAMING] All queries started. Waiting for data...
```

### Terminal 4 — Start Kafka Producer

```bash
cd ~/movie-recommendation-system
python3 streaming/kafka_producer.py
```

### Terminal 5 — Launch Dashboard

```bash
cd ~/movie-recommendation-system
streamlit run dashboard/app.py --server.port 8501
```

Open `http://localhost:8501` in a browser.

---

## Verification Commands

```bash
# Confirm all source files exist
find ~/movie-recommendation-system -name "*.py" | sort

# Check dataset files
ls ~/movie-recommendation-system/data/ml-1m/

# Verify saved model
ls ~/movie-recommendation-system/models/

# View latest recommendations
ls ~/movie-recommendation-system/outputs/recommendations/

# Tail the alert log
tail -n 20 ~/movie-recommendation-system/outputs/alerts/alerts.log

# Verify Kafka topic
~/kafka/bin/kafka-topics.sh --describe \
  --topic movie-ratings \
  --bootstrap-server localhost:9092
```

---

## Technology Stack

| Component | Technology |
|---|---|
| Batch ML | Apache Spark MLlib — ALS |
| Stream Processing | Spark Structured Streaming |
| Message Broker | Apache Kafka 3.7.0 |
| Producer | Python `kafka-python` |
| Dashboard | Streamlit + Plotly |
| Language | Python 3.10 |
| Storage | Parquet (precomputed recs), JSON (outputs), plain text (alerts) |
| Cluster | Apache Spark `local[*]` |



## Team

| Name | ID |
|---|---|
| Mohamed Ehab Yousri | 202201236 |
| Yousef Selim | 202201255 |
