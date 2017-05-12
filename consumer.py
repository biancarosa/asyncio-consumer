import asyncio
import asynqp


class Consumer(object):

    def __init__(self):
        self.connection = None
        self.channel = None

    @asyncio.coroutine
    def start(self, loop):
        try:
            yield from self.setup()
            yield from self.consume()
        except Exception:
            yield from asyncio.sleep(1)
            loop.create_task(self.start(loop))

    @asyncio.coroutine
    def setup(self):
        self.connection = yield from asynqp.connect('localhost', 5672, username='guest', password='guest')
        self.channel = yield from self.connection.open_channel()
        print("Connected")

    def callback(self, msg):
        print("Got message")
        print(msg)
        msg.ack()

    @asyncio.coroutine
    def consume(self):
        exchange = yield from self.channel.declare_exchange('test.exchange', 'direct')
        queue = yield from self.channel.declare_queue('test.queue')
        print("Consuming")
        consumer = yield from queue.consume(self.callback)
        return consumer

    @asyncio.coroutine
    def close(self):
        print("Closing")
        yield from self.channel.close()
        yield from self.connection.close()
        print("Shutdown complete ...")

    def exception_handler(self, loop, context):
        close_task = loop.create_task(self.close())
        asyncio.wait_for(close_task, None)

if __name__ == "__main__":
    consumer = Consumer()
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(consumer.exception_handler)
    loop.create_task(consumer.start(loop))
    loop.run_forever()
