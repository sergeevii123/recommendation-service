import redis
import time
import asyncio
import os
import polars as pl
import logging
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

async def calculate_top_recommendations():
    while True:
        if os.path.exists(INTERACTIONS_FILE):
            logging.info('calculating top recommendations')
            interactions = pl.read_csv(INTERACTIONS_FILE)
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
            logging.debug(f'top_items: {top_items}')
            redis_connection.json().set('top_items', '.', top_items)
        await asyncio.sleep(10)

if __name__ == '__main__':
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    asyncio.run(calculate_top_recommendations())