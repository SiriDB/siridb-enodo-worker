import argparse
import asyncio

from worker import Worker

parser = argparse.ArgumentParser(description='Process config')
parser.add_argument('--config', help='Config path', required=True)

loop = asyncio.get_event_loop()
worker = Worker(loop, parser.parse_args().config)
loop.run_until_complete(worker.start_worker())
loop.run_forever()
loop.close()