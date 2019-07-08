import asyncio
import configparser
import json
from queue import Queue
from threading import Thread

from lib.analyser.analyser import start_analyser_worker
from lib.socket.client import Client
from lib.socket.package import *


class Worker:

    def __init__(self, loop, config_path):
        self._loop = loop
        self._config = configparser.ConfigParser()
        self._config.read(config_path)
        self._series_to_watch = ()
        self._serie_counter_updates = {}
        self._client = Client(loop, self._config['enodo']['hub_hostname'], int(self._config['enodo']['hub_port']),
                              'worker', heartbeat_interval=int(self._config['enodo']['heartbeat_interval']))
        self._client_run_task = None
        self._updater_task = None
        self._result_queue = Queue()
        self._busy = False

    async def _update_busy(self, busy):
        self._busy = busy
        await self._client.send_message(str(busy).encode('utf-8'), WORKER_UPDATE_BUSY)

    async def _check_for_update(self):
        while not self._result_queue.empty():
            pkl = self._result_queue.get()
            await self._send_update(pkl)
            await self._update_busy(False)

    async def _send_update(self, pkl):
        await self._client.send_message(pkl, WORKER_RESULT)

    async def _start_analyse(self, serie_name):
        await self._update_busy(True)
        worker_loop = asyncio.new_event_loop()
        worker = Thread(target=start_analyser_worker, args=(worker_loop, self._result_queue, serie_name, self._config,))
        # Start the thread
        worker.start()

    async def _receive_forecast_request(self, writer, packet_type, packet_id, data, client_id):
        data = json.loads(data.decode("utf-8"))
        print('GOT FORECAST REQUEST')
        print(data)
        pass

    async def start_worker(self):
        await self._client.setup(cbs={
            # UPDATE_SERIES: self._handle_update_series
            FORECAST_SERIE: self._receive_forecast_request
        })
        self._client_run_task = self._loop.create_task(self._client.run())
        self._updater_task = self._loop.create_task(self._check_for_update())

    def close(self):
        self._client_run_task.cancel()
        self._updater_task.cancel()
        self._loop.run_until_complete(self._client_run_task)
        self._loop.run_until_complete(self._updater_task)
