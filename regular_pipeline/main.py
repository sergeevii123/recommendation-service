import asyncio
import json
import os.path
import time

import aio_pika
import aiormq
import polars as pl
from aio_pika import Message
from recs import calculate_recommendations, INTERACTIONS_FILE
import logging

async def get_rabbitmq_connection():
    for _ in range(10):
        logging.info('trying to connect to rabbitmq')
        try:
            connection = await aio_pika.connect_robust(
                "amqp://guest:guest@rabbitmq/",
                loop=asyncio.get_event_loop()
            )
        except aiormq.exceptions.AMQPConnectionError as e:
            logging.info('rabbitmq is not ready yet')
            await asyncio.sleep(2)
            continue
        logging.info('rabbitmq is connected')
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

        t_start = time.time()
        data = []
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    message = message.body.decode()
                    if time.time() - t_start > 10:
                        logging.info('saving events from rabbitmq')
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


async def main():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    await asyncio.gather(
        collect_messages(),
        calculate_recommendations(),
    )


if __name__ == '__main__':
    asyncio.run(main())
