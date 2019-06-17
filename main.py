import asyncio

from worker import Worker

loop = asyncio.get_event_loop()
listener = Worker(loop)
loop.run_until_complete(listener.start_listener())
loop.run_forever()
loop.close()