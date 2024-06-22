import random
from typing import List

import numpy as np
import redis
from fastapi import FastAPI

from models import InteractEvent, RecommendationsResponse, NewItemsEvent
from watched_filter import WatchedFilter
from time import sleep
import uvicorn

def get_redis_connection():
    for _ in range(10):
        try:
            redis_connection = redis.Redis('redis')
        except redis.exceptions.ConnectionError:
            print('redis is not ready yet')
            sleep(2)
            continue
        return redis_connection


app = FastAPI()

redis_connection = get_redis_connection()
watched_filter = WatchedFilter()
print('ready')
unique_item_ids = set()
EPSILON = 0.05


@app.get('/healthcheck')
def healthcheck():
    return 200


@app.get('/cleanup')
def cleanup():
    global unique_item_ids
    unique_item_ids = set()
    try:
        redis_connection.delete('*')
        redis_connection.json().delete('*')
    except redis.exceptions.ConnectionError:
        pass
    return 200


@app.post('/add_items')
def add_movie(request: NewItemsEvent):
    global unique_item_ids
    for item_id in request.item_ids:
        unique_item_ids.add(item_id)
    return 200


@app.get('/recs/{user_id}')
def get_recs(user_id: str):
    global unique_item_ids

    try:
        item_ids = redis_connection.json().get('combined_recommendations')
    except redis.exceptions.ConnectionError:
        item_ids = None

    if item_ids is None or random.random() < EPSILON:
        item_ids = np.random.choice(list(unique_item_ids), size=20, replace=False).tolist()
    return RecommendationsResponse(item_ids=item_ids)


@app.post('/interact')
async def interact(request: InteractEvent):
    watched_filter.add(request.user_id, request.item_id)
    return 200


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5001)
