import asyncio
import json
import os.path
import time

import aio_pika
import aiormq
import polars as pl
import redis
from aio_pika import Message

def get_redis_connection():
    for _ in range(10):
        try:
            redis_connection = redis.Redis('redis')
        except redis.exceptions.ConnectionError:
            print('redis is not ready yet')
            time.sleep(2)
            continue
        return redis_connection


redis_connection = get_redis_connection()

INTERACTIONS_FILE = 'data/interactions.csv'

async def get_rabbitmq_connection():
    for _ in range(10):
        print('trying to connect to rabbitmq')
        try:
            connection = await aio_pika.connect_robust(
                "amqp://guest:guest@rabbitmq/",
                loop=asyncio.get_event_loop()
            )
        except aiormq.exceptions.AMQPConnectionError as e:
            print('rabbitmq is not ready yet')
            await asyncio.sleep(2)
            continue
        print('rabbitmq is connected')
        return connection

async def collect_messages():
    connection = await get_rabbitmq_connection()

    queue_name = "user_interactions"
    routing_key = "user.interact.message"

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Will take no more than 10 messages in advance
        await channel.set_qos(prefetch_count=10)

        # Declaring queue
        queue = await channel.declare_queue(queue_name)

        # Declaring exchange
        exchange = await channel.declare_exchange("user.interact", type='direct')
        await queue.bind(exchange, routing_key)
        # await exchange.publish(Message(bytes(queue.name, "utf-8")), routing_key)

        t_start = time.time()
        data = []
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    message = message.body.decode()
                    if time.time() - t_start > 10:
                        print('saving events from rabbitmq')
                        # update data if 10s passed
                        if len(data) > 0:
                            new_data = pl.DataFrame(data).explode(['item_ids', 'actions']).rename({
                                'item_ids': 'item_id',
                                'actions': 'action'
                            })

                            if len(new_data) > 0:
                                if os.path.exists(INTERACTIONS_FILE):
                                    data = pl.concat([pl.read_csv(INTERACTIONS_FILE), new_data])
                                else:
                                    data = new_data
                                data.write_csv(INTERACTIONS_FILE)

                            data = []
                            t_start = time.time()

                    message = json.loads(message)
                    data.append(message)


async def calculate_top_recommendations():
    while True:
        if os.path.exists(INTERACTIONS_FILE):
            print('calculating top recommendations')
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

            redis_connection.json().set('top_items', '.', top_items)
        await asyncio.sleep(10)


async def main():
    await asyncio.gather(
        collect_messages(),
        calculate_top_recommendations(),
    )


if __name__ == '__main__':
    asyncio.run(main())
