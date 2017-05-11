import asyncio
import asynqp


@asyncio.coroutine
def produce():
    """
    Sends a 'hello world' message and then reads it from the queue.
    """
    # connect to the RabbitMQ broker
    connection = yield from asynqp.connect('localhost', 5672, username='guest', password='guest')

    # Open a communications channel
    channel = yield from connection.open_channel()

    # Create a queue and an exchange on the broker
    exchange = yield from channel.declare_exchange('test.exchange', 'direct')
    queue = yield from channel.declare_queue('test.queue')

    # Bind the queue to the exchange, so the queue will get messages published to the exchange
    yield from queue.bind(exchange, 'routing.key')

    for i in range(1,50000):
        # If you pass in a dict it will be automatically converted to JSON
        msg = asynqp.Message({'hello': 'world'})
        exchange.publish(msg, 'routing.key')

    yield from channel.close()
    yield from connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(produce())
