import argparse
import asyncio
import signal
import os
import logging

from worker import Worker
from lib.config import create_standard_config_file

parser = argparse.ArgumentParser(description='Process config')
parser.add_argument('--log_level', help='Log level, error/warning/info/debug, default: info', required=False,
                        default='info')
parser.add_argument('--config', help='Config path', required=False)
parser.add_argument('--create_config', help='Create standard config file', action='store_true', default=False)

if parser.parse_args().create_config:
    create_standard_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default.conf'))
    exit()


async def shutdown(signal, loop, worker):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {signal.name}...")
    logging.info("Sending Hub that we are going down")
    await worker.shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(1)
    loop.stop()


loop = asyncio.get_event_loop()
worker = Worker(loop, parser.parse_args().config, parser.parse_args().log_level)

signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
for s in signals:
    loop.add_signal_handler(
        s, lambda s=s: asyncio.create_task(shutdown(s, loop, worker)))

try:
    loop.run_until_complete(worker.start_worker())
    loop.run_forever()
except asyncio.CancelledError:
        logging.debug('Tasks has been canceled')
finally:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)
    loop.close()
