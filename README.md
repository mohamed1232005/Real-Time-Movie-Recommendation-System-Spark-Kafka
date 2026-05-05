# 🎬 Real-Time Movie Recommendation System

**Big Data Analytics — Mini Project 3**  
Apache Spark + Kafka + ALS Collaborative Filtering

---

## 👥 Team

| Name | ID |
|------|----|
| Mohamed Ehab | s-mohamed.ehab |

---

## 🎯 Project Overview

This project builds a complete big data pipeline combining:

- Batch machine learning (ALS)
- Real-time streaming (Kafka + Spark)
- Live recommendations + alerts

Dataset: MovieLens 1M  
Records: 1,000,209  
Users: 6,040  
Movies: 3,706  
Sparsity: 95.53%

---

## 🏗️ Architecture

MovieLens Dataset → Preprocessing → ALS Model → Saved Model  
MovieLens Dataset → Kafka Producer → Kafka Topic → Spark Streaming → Outputs

Outputs:
- Recommendations
- Alerts
- Dashboard

---

## 📁 Project Structure

movie-recommendation-system/
├── config/
├── data/ml-1m/
├── batch/
├── streaming/
├── integration/
├── dashboard/
├── models/
├── checkpoints/
├── outputs/
├── requirements.txt
└── README.md

---

## ⚙️ Setup

### Install dependencies
pip3 install -r requirements.txt

### Dataset
Place files in:
data/ml-1m/

Files:
- ratings.dat
- movies.dat
- users.dat

---

## 🚀 Run Steps

### 1. Start Kafka

Terminal 1:
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties

Terminal 2:
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties

---

### 2. Train Model
python3 batch/als_training.py

Expected:
RMSE ≈ 0.8758

---

### 3. Start Streaming
python3 streaming/spark_streaming.py

---

### 4. Start Producer
python3 streaming/kafka_producer.py

---

### 5. Dashboard
streamlit run dashboard/app.py --server.port 8501

---

## 🧠 Machine Learning

Algorithm: ALS

Parameters:
- rank = 10
- regParam = 0.1
- maxIter = 10

Train/Test:
- 80% / 20%

Result:
RMSE = 0.8758

---

## 📡 Streaming

Kafka Topic:
- name: movie-ratings
- partitions: 2

Partitioning:
item_id % 2

---

## ⏱️ Window Settings

- Window: 30 seconds
- Slide: 10 seconds
- Watermark: 15 seconds

---

## 🔥 Custom Metric

Trending Score:

interaction_count × avg_rating

---

## 🚨 Alerts

1. Rating Spike → avg_rating ≥ 4.5  
2. Trending → score ≥ 30  
3. User Spike → interactions ≥ 10  

---

## 🔗 Integration

Known user:
→ ALS recommendations

New user:
→ Popular movies (cold-start)

---

## 📊 Dashboard

Panels:
- Recommendations
- Trending movies
- User activity
- Alerts
- Metrics

---

## 📈 Results

- RMSE: 0.8758
- Training time: ~57s
- Latency: ~800–3500 ms
- Dataset size: 1M+

---

## 🛠️ Technologies

- Apache Spark
- Kafka
- Python
- Streamlit
- Plotly

---

## ✅ Status

✔ Batch ML  
✔ Streaming  
✔ Kafka  
✔ ALS Model  
✔ Alerts  
✔ Dashboard  

---

## 📌 Summary

This system demonstrates a real-time recommendation pipeline using Spark and Kafka.

It processes streaming movie ratings, detects trends, and generates recommendations with low latency.

Final RMSE:

0.8758

