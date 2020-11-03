import argparse
import asyncio
import signal

from worker import Worker

parser = argparse.ArgumentParser(description='Process config')
parser.add_argument('--config', help='Config path', required=True)


async def shutdown(signal, loop, worker):
    """Cleanup tasks tied to the service's shutdown."""
    print(f"Received exit signal {signal.name}...")
    print("Sending Hub that we are going down")
    await worker.shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    print(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(1)
    loop.stop()


loop = asyncio.get_event_loop()
worker = Worker(loop, parser.parse_args().config)

signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
for s in signals:
    loop.add_signal_handler(
        s, lambda s=s: asyncio.create_task(shutdown(s, loop, worker)))

try:
    loop.run_until_complete(worker.start_worker())
    loop.run_forever()
except asyncio.CancelledError:
        print('Tasks has been canceled')
finally:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)
    loop.close()
