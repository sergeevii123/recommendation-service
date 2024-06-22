import redis
import time
import asyncio
import os
import polars as pl
import logging
import numpy as np
from implicit.als import AlternatingLeastSquares
from scipy.sparse import coo_matrix
from sklearn.preprocessing import LabelEncoder
import random

random.seed(42)
INTERACTIONS_FILE = 'data/interactions.csv'

def get_redis_connection():
    for _ in range(10):
        try:
            redis_connection = redis.Redis('redis')
        except redis.exceptions.ConnectionError:
            logging.info('redis is not ready yet')
            time.sleep(2)
            continue
        return redis_connection

redis_connection = get_redis_connection()

def calculate_als_recommendations(interactions):
    try:
        interactions = interactions.filter(pl.col('action') == 'like')
        user_ids = interactions['user_id'].to_list()
        item_ids = interactions['item_id'].to_list()

        # Encode user_ids and item_ids as integers
        user_encoder = LabelEncoder()
        item_encoder = LabelEncoder()
        user_ids_encoded = user_encoder.fit_transform(user_ids)
        item_ids_encoded = item_encoder.fit_transform(item_ids)

        data = [1] * len(user_ids)  # implicit feedback, binary likes

        user_item_matrix = coo_matrix((data, (user_ids_encoded, item_ids_encoded)))

        model = AlternatingLeastSquares(factors=20, regularization=0.1, iterations=20)
        model.fit(user_item_matrix.T)

        item_factors = model.item_factors
        user_factors = model.user_factors

        scores = np.dot(user_factors, item_factors.T)
        top_als_items = np.argsort(scores.sum(axis=0))[::-1][:10]  # Limit to top 10 items

        # Filter out items that were not seen during the fit process
        seen_labels = set(item_encoder.classes_)
        top_als_items = [item for item in top_als_items if item in seen_labels]

        top_als_items = item_encoder.inverse_transform(top_als_items)  # Decode to original item_ids
        top_als_items = [str(item_id) for item_id in top_als_items]
    except Exception as e:
        logging.error(f'Error calculating ALS recommendations: {e}')
        top_als_items = []
    return top_als_items

def calculate_top_items(interactions):
    top_items = (
        interactions
        .sort('timestamp')
        .unique(['user_id', 'item_id', 'action'], keep='last')
        .filter(pl.col('action') == 'like')
        .groupby('item_id')
        .count()
        .sort('count', descending=True)
        .head(100)
    )['item_id'].to_list()
    top_items = [str(item_id) for item_id in top_items]
    return top_items

def get_random_items(interactions, exclude_items, count=1):
    all_items = interactions['item_id'].unique().to_list()
    all_items = [str(item) for item in all_items if str(item) not in exclude_items]
    random_items = random.sample(all_items, min(count, len(all_items)))
    return random_items

def combine_recommendations(top_items, als_items, interactions, max_recommendations=10):
    combined_recommendations = als_items
    if len(combined_recommendations) < max_recommendations:
        combined_recommendations += [item for item in top_items if item not in combined_recommendations]
    combined_recommendations = combined_recommendations[:max_recommendations-1]  # Reserve last slot for random item
    combined_recommendations += get_random_items(interactions, combined_recommendations, 1)
    return combined_recommendations

async def calculate_recommendations():
    while True:
        if os.path.exists(INTERACTIONS_FILE):
            logging.info('calculating recommendations')
            interactions = pl.read_csv(INTERACTIONS_FILE)

            # Calculate top items and ML-based recommendations
            top_items = calculate_top_items(interactions)
            als_items = calculate_als_recommendations(interactions)

            # Combine recommendations
            combined_recommendations = combine_recommendations(top_items, als_items, interactions)

            logging.debug(f'combined_recommendations: {combined_recommendations}')
            redis_connection.json().set('combined_recommendations', '.', combined_recommendations)
        await asyncio.sleep(10)

if __name__ == '__main__':
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    asyncio.run(calculate_recommendations())