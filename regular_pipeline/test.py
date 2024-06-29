import asyncio
from recs import periodic_task, calculate_top_items, calculate_lightfm_recommendations, update_unseen_random_items, INTERACTIONS_FILE
import os
import logging
import polars as pl

async def task(task_func):
    if os.path.exists(INTERACTIONS_FILE):
        logging.info(f'Running task: {task_func.__name__}')
        interactions = pl.read_csv(INTERACTIONS_FILE)
        await task_func(interactions)
        logging.info(f'Task {task_func.__name__} completed')


async def calculate_recommendations():
    await asyncio.gather(
        # task(calculate_top_items, 5), 
        task(calculate_lightfm_recommendations),
        # task(update_unseen_random_items, 5) 
    )

if __name__ == '__main__':
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    asyncio.run(calculate_recommendations())
