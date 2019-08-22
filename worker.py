import asyncio
import configparser
import datetime
import json
import pickle
from queue import Queue
from threading import Thread

from lib.analyser.analyser import start_analysing
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
        self._started_last_job = None
        self._max_job_duration = self._config['enodo']['max_job_duration']
        self._worker_thread = None

    async def _update_busy(self, busy):
        self._busy = busy
        await self._client.send_message(str(busy).encode('utf-8'), WORKER_UPDATE_BUSY)

    async def _check_for_update(self):
        while True:
            if not self._result_queue.empty():
                try:
                    pkl = json.dumps(self._result_queue.get()).encode('utf-8')
                    print(pkl)
                    await self._send_update(pkl)
                    await self._update_busy(False)
                except Exception as e:
                    print(e)
            await asyncio.sleep(2)

    async def _send_update(self, pkl):
        await self._client.send_message(pkl, WORKER_RESULT)

    async def _receive_forecast_request(self, data):
        try:
            data = pickle.loads(data)
            print(f'Received forecast request for serie: "{data.get("serie_name")}"')
            print(data)
            await self._update_busy(True)

            worker_loop = asyncio.new_event_loop()
            self._worker_thread = Thread(target=start_analysing, args=(worker_loop, self._result_queue, self._max_job_duration, data.get("serie_name"),
                                                          data.get('wrapper'),
                                                          self._config['siridb']['user'],
                                                          self._config['siridb']['password'],
                                                          self._config['siridb']['database'],
                                                          self._config['siridb']['host'],
                                                          self._config['siridb']['port'],))
            # Start the thread
            self._worker_thread.start()
        except Exception as e:
            print("WOW: ", e)

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
