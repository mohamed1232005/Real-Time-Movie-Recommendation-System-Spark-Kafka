import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *


ALERT_LOG = os.path.join(OUTPUT_ALERTS, "alerts.log")
os.makedirs(OUTPUT_ALERTS, exist_ok=True)


def _log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(ALERT_LOG, "a") as f:
        f.write(line + "\n")


def check_rating_spike(item_id, avg_rating, movie_title="Unknown"):
    if avg_rating >= ALERT_RATING_THRESHOLD:
        _log(
            f"🔴 ALERT [RATING SPIKE] "
            f"Item {item_id} ({movie_title}) "
            f"avg_rating={avg_rating:.2f} > {ALERT_RATING_THRESHOLD}"
        )
        return True
    return False


def check_trending(item_id, trending_score, movie_title="Unknown"):
    if trending_score >= TRENDING_SCORE_THRESHOLD:
        _log(
            f"🔥 ALERT [TRENDING] "
            f"Item {item_id} ({movie_title}) "
            f"trending_score={trending_score:.2f} > {TRENDING_SCORE_THRESHOLD}"
        )
        return True
    return False


def check_user_activity(user_id, interaction_count):
    if interaction_count >= ALERT_INTERACTION_THRESHOLD:
        _log(
            f"⚡ ALERT [USER SPIKE] "
            f"User {user_id} has {interaction_count} interactions "
            f"in window (threshold={ALERT_INTERACTION_THRESHOLD})"
        )
        return True
    return False


def evaluate_window_alerts(window_df, movies_lookup=None):
    alerts_fired = 0
    for row in window_df:
        item_id           = row.get("item_id", -1)
        avg_rating        = row.get("avg_rating", 0.0)
        interaction_count = row.get("interaction_count", 0)
        trending_score    = row.get("trending_score", 0.0)
        title = (movies_lookup or {}).get(item_id, "Unknown")

        if check_rating_spike(item_id, avg_rating, title):
            alerts_fired += 1
        if check_trending(item_id, trending_score, title):
            alerts_fired += 1

    return alerts_fired
