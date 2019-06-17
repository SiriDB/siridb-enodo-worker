import asyncio
import configparser
import json
import os
from queue import Queue
from threading import Thread

from lib.analyser.analyser import start_analyser_worker
from lib.socket.client import Client
from lib.socket.package import *


class Worker:

    def __init__(self, loop):
        self._loop = loop
        self._config = configparser.ConfigParser()
        self._config.read(os.path.join(os.path.dirname(__file__), 'listener.conf'))
        self._series_to_watch = ()
        self._serie_counter_updates = {}
        self._client = Client(loop, self._config['enodo']['hub_hostname'], int(self._config['enodo']['hub_port']),
                              heartbeat_interval=int(self._config['enodo']['heartbeat_interval']))
        self._client_run_task = None
        self._updater_task = None
        self._result_queue = Queue()
        self._busy = False

    async def _send_update(self):
        update_encoded = json.dumps(self._serie_counter_updates).encode('utf-8')
        self._serie_counter_updates = {}
        await self._client.send_message(update_encoded, LISTENER_ADD_SERIE_COUNT)

    async def _start_analyse(self):
        worker_loop = asyncio.new_event_loop()
        worker = Thread(target=start_analyser_worker, args=(worker_loop,))
        # Start the thread
        worker.start()

    async def start_listener(self):
        await self._client.setup(cbs={
            UPDATE_SERIES: self._handle_update_series
        })
        self._client_run_task = self._loop.create_task(self._client.run())

        # self._updater_task = self._loop.create_task(self._updater())

    def close(self):
        self._client_run_task.cancel()
        self._updater_task.cancel()
        self._loop.run_until_complete(self._client_run_task)
        self._loop.run_until_complete(self._updater_task)
