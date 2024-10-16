import redis
import time
import asyncio
import os
import polars as pl
import logging
import numpy as np
from scipy.sparse import coo_matrix, csr_matrix
from sklearn.preprocessing import LabelEncoder
import random
from lightfm import LightFM

RANDOM_STATE = 42
TOP_K = 10
random.seed(RANDOM_STATE)
INTERACTIONS_FILE = "data/interactions.csv"


def get_redis_connection():
    for _ in range(10):
        try:
            redis_connection = redis.Redis(host="redis", decode_responses=True)
            return redis_connection
        except redis.exceptions.ConnectionError:
            logging.info("Redis is not ready yet")
            time.sleep(2)
    raise Exception("Failed to connect to Redis")


redis_connection = get_redis_connection()


def build_user_item_matrix(interactions):
    """Build user-item matrix from df of interactions"""
    rows, cols, values = [], [], []
    for row in interactions.iter_rows(named=True):
        user_id = row["user_id_encoded"]
        item_id = row["item_id_encoded"]
        action = row["action"]
        rows.append(user_id)
        cols.append(item_id)
        values.append(1 if action == "like" else -1)
    user_item_data = csr_matrix((values, (rows, cols)), dtype=np.float32)
    return user_item_data


def calculate_lightfm_model(user_item_data):
    """Calculates LightFM on user-item matrix"""
    try:
        model = LightFM(
            no_components=38,
            loss="warp",
            item_alpha=8.551911304867999e-05,
            user_alpha=8.551911304867999e-05,
            random_state=RANDOM_STATE,
        )
        model.fit(user_item_data, epochs=13, verbose=True)
        return model
    except Exception as e:
        logging.error(f"Error calculating LightFM model: {e}")
        return None


def get_recommendations(model, user_item_data, user_ids, k=TOP_K):
    """Returns top k recommendations for user based on LightFM model"""
    n_users, n_items = user_item_data.shape
    recommendations = []
    for user_id in user_ids:
        scores = model.predict(user_id, np.arange(n_items))
        top_items = np.argsort(-scores)[:k]
        recommendations.append(top_items)
    return recommendations


async def calculate_top_items(interactions):
    """Writes top 20 items to redis based on number of likes"""
    try:
        top_items = (
            interactions.sort("timestamp")
            .unique(["user_id", "item_id", "action"], keep="last")
            .filter(pl.col("action") == "like")
            .groupby("item_id")
            .count()
            .sort("count", descending=True)
            .head(TOP_K)
        )["item_id"].to_list()
        top_items = [str(item_id) for item_id in top_items]
        redis_connection.json().set("top_items", ".", top_items)
    except Exception as e:
        logging.error(f"Error calculating top items: {e}")


def get_unseen_random_items(interactions, exclude_items, count=1):
    """Returns random items that user hasn't seen yet"""
    all_items = interactions["item_id"].unique().to_list()
    all_items = [str(item) for item in all_items if str(item) not in exclude_items]
    random_items = random.sample(all_items, min(count, len(all_items)))
    return random_items


async def update_unseen_random_items(interactions):
    """Writes 10 random unseen items for every user tor redis"""
    try:
        user_ids = interactions["user_id"].unique().to_list()
        for user_id in user_ids:
            seen_items = (
                interactions.filter(pl.col("user_id") == user_id)["item_id"]
                .unique()
                .to_list()
            )
            random_items = get_unseen_random_items(
                interactions, seen_items, count=TOP_K
            )
            redis_connection.json().set(
                f"unseen_random_items:{user_id}", ".", random_items
            )
    except Exception as e:
        logging.error(f"Error updating random items: {e}")


async def calculate_lightfm_recommendations(interactions):
    """Writes recs for users to redis based on LightFM model"""
    try:
        user_encoder = LabelEncoder()
        item_encoder = LabelEncoder()

        user_ids_encoded = user_encoder.fit_transform(interactions["user_id"].to_list())
        item_ids_encoded = item_encoder.fit_transform(interactions["item_id"].to_list())

        interactions = interactions.with_columns(
            [
                pl.Series("user_id_encoded", user_ids_encoded).cast(pl.Int32),
                pl.Series("item_id_encoded", item_ids_encoded).cast(pl.Int32),
            ]
        )

        user_item_data = build_user_item_matrix(interactions)
        model = calculate_lightfm_model(user_item_data)
        if model is not None:
            recs = get_recommendations(
                model,
                user_item_data,
                interactions["user_id_encoded"].unique().to_list(),
                TOP_K,
            )
            user_ids = interactions["user_id"].unique().to_list()
            for user_idx, user_id in enumerate(user_ids):
                try:
                    top_items_indices = recs[user_idx]
                    top_item_ids = item_encoder.inverse_transform(top_items_indices)
                    top_item_ids_str = [str(item_id) for item_id in top_item_ids]
                    redis_connection.json().set(
                        f"lightfm_recommendations:{user_id}", ".", top_item_ids_str
                    )
                except Exception as e:
                    logging.error(
                        f"Error processing recommendations for user {user_id}: {e}"
                    )
    except Exception as e:
        logging.error(f"Error in calculate_lightfm_recommendations: {e}")


async def periodic_task(task_func, sleep_time):
    while True:
        if os.path.exists(INTERACTIONS_FILE):
            interactions = pl.read_csv(INTERACTIONS_FILE)
            logging.info(
                f"Running task: {task_func.__name__} len interactions {len(interactions)}"
            )
            await task_func(interactions)
            logging.info(f"Task {task_func.__name__} completed")
        await asyncio.sleep(sleep_time)


async def calculate_recommendations():
    await asyncio.gather(
        periodic_task(calculate_top_items, 5),
        periodic_task(calculate_lightfm_recommendations, 30),
        periodic_task(update_unseen_random_items, 5),
    )


if __name__ == "__main__":
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    asyncio.run(calculate_recommendations())
