import asyncio
import asynqp


class Consumer(object):

    def __init__(self):
        self.connection = None
        self.channel = None

    def callback(self, msg):
        print("Got message")
        print(msg)
        msg.ack()

    @asyncio.coroutine
    def consume(self):
        """
        Sends a 'hello world' message and then reads it from the queue.
        """
        print("Closing")
        self.connection = yield from asynqp.connect('localhost', 5672, username='guest', password='guest')
        self.channel = yield from self.connection.open_channel()
        exchange = yield from self.channel.declare_exchange('test.exchange', 'direct')
        queue = yield from self.channel.declare_queue('test.queue')
        print("Consuming")
        yield from queue.consume(self.callback)

    @asyncio.coroutine
    def close(self):
        print("Closing")
        yield from self.channel.close()
        yield from self.connection.close()

if __name__ == "__main__":
    consumer = Consumer()
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(consumer.consume())
        loop.run_forever()
    finally:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(consumer.close())
        loop.close()
