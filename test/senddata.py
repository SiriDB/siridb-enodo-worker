import asyncio
import json
import time
import random
from siridb.connector import SiriDBClient

async def example(siri):
    # Start connecting to siridb.
    # .connect() returns a list of all connections referring to the supplied
    # hostlist. The list can contain exceptions in case a connection could not
    # be made.
    await siri.connect()

    try:
        await siri.insert({'hub_test1': [[1560350480175, 1]]})
    finally:
        # Close all siridb connections.
        siri.close()


siri = SiriDBClient(
    username='iris',
    password='siri',
    dbname='testdata_1',
    hostlist=[('localhost', 9000)],  # Multiple connections are supported
    keepalive=True)

loop = asyncio.get_event_loop()
loop.run_until_complete(example(siri))