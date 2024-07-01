import random
from typing import List
import numpy as np
import redis
from fastapi import FastAPI
import logging
from models import InteractEvent, RecommendationsResponse, NewItemsEvent
from watched_filter import WatchedFilter
from time import sleep
import uvicorn
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_client import start_http_server

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
TOP_K = 10

# Prometheus metrics
REQUEST_COUNT = Counter('request_count', 'App Request Count', ['method', 'endpoint'])
RECOMMENDATION_SOURCE = Counter('recommendation_source', 'Source of recommendations', ['source'])
ERROR_COUNT = Counter('error_count', 'App Error Count', ['endpoint'])
REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency', ['endpoint'])

ENABLE_LIGHTFM_RECS = True
ENABLE_UNSEEN_RANDOM_RECS = True
ENABLE_TOP_RECS = True

@app.get('/metrics')
def metrics():
    return generate_latest()

@app.get('/healthcheck')
def healthcheck():
    REQUEST_COUNT.labels(method='GET', endpoint='/healthcheck').inc()
    return 200

@app.get('/cleanup')
def cleanup():
    REQUEST_COUNT.labels(method='GET', endpoint='/cleanup').inc()
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
    REQUEST_COUNT.labels(method='POST', endpoint='/add_items').inc()
    global unique_item_ids
    for item_id in request.item_ids:
        unique_item_ids.add(item_id)
    return 200

def add_unseen_random_items(item_ids: List[str], unseen_random_items: List[str], source:str):
    if unseen_random_items and ENABLE_UNSEEN_RANDOM_RECS:
        starting_size = len(item_ids)
        item_ids.extend(unseen_random_items[:TOP_K-starting_size])
        source += f'Random unseen {TOP_K-starting_size} '
        RECOMMENDATION_SOURCE.labels(source='Random unseen').inc(TOP_K-starting_size)
    return item_ids, source

@app.get('/recs/{user_id}')
def get_recs(user_id: str):
    with REQUEST_LATENCY.labels(endpoint='/recs').time():
        REQUEST_COUNT.labels(method='GET', endpoint='/recs').inc()
        global unique_item_ids
        top_items = None
        lightfm_items = None
        unseen_random_items = None

        try:
            top_items = redis_connection.json().get('top_items')
            lightfm_items = redis_connection.json().get(f'lightfm_recommendations:{user_id}')
            unseen_random_items = redis_connection.json().get(f'unseen_random_items:{user_id}')
        except redis.exceptions.ConnectionError:
            logging.error('Redis is not ready yet')
            ERROR_COUNT.labels(endpoint='/recs').inc()

        item_ids = []
        source = ''
        if lightfm_items and ENABLE_LIGHTFM_RECS:
            item_ids.extend(lightfm_items[:TOP_K-2])
            source += f'LightFM {len(item_ids)} '
            RECOMMENDATION_SOURCE.labels(source='LightFM').inc(len(item_ids))
            item_ids, source = add_unseen_random_items(item_ids, unseen_random_items, source)
        elif top_items and ENABLE_TOP_RECS:
            item_ids.extend(top_items[:TOP_K//2])
            source += f'TOP {len(item_ids)} '
            RECOMMENDATION_SOURCE.labels(source='TOP').inc(len(item_ids))
            item_ids, source = add_unseen_random_items(item_ids, unseen_random_items, source)
        else:
            item_ids, source = add_unseen_random_items(item_ids, unseen_random_items, source)
        
        if random.random() < EPSILON:
            item_ids = np.random.choice(list(unique_item_ids), size=TOP_K, replace=False).tolist()
            source = f'Random {len(item_ids)} '
            RECOMMENDATION_SOURCE.labels(source='Random').inc(len(item_ids))
        elif len(item_ids) < TOP_K:
            # добавляем случайные айтемы, если не хватает
            starting_size = len(item_ids)
            item_ids.extend(np.random.choice(list(unique_item_ids-set(item_ids)), size=TOP_K-starting_size, replace=False).tolist())
            source += f'Random {TOP_K - starting_size} '
            RECOMMENDATION_SOURCE.labels(source='Random').inc(TOP_K - starting_size)
        
        logging.info(f'Final recommendations for user {user_id}:{item_ids} Source: {source}')
        return RecommendationsResponse(item_ids=item_ids)

@app.post('/interact')
async def interact(request: InteractEvent):
    REQUEST_COUNT.labels(method='POST', endpoint='/interact').inc()
    watched_filter.add(request.user_id, request.item_id)
    return 200

if __name__ == "__main__":
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    # Start Prometheus metrics server
    start_http_server(8000)
    uvicorn.run(app, host="0.0.0.0", port=5001)
