import asyncio
import configparser
import traceback

from queue import Queue
from threading import Thread
from enodo.client import Client
from enodo.client.package import *

from lib.analyser.analyser import start_analysing, AnalyserWrapper


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
        self._running = True

    async def _update_busy(self, busy):
        self._busy = busy
        await self._client.send_message(busy, WORKER_UPDATE_BUSY)

    async def _send_refused(self):
        await self._client.send_message(None, WORKER_REFUSED)

    async def _send_shutdown(self):
        await self._client.send_message(None, CLIENT_SHUTDOWN)

    async def _check_for_update(self):
        while self._running:
            if not self._result_queue.empty():
                try:
                    pkl = self._result_queue.get()
                except Exception as e:
                    print(e)
                    pass
                else:
                    print(pkl)
                    await self._send_update(pkl)
                    await self._update_busy(False)
            await asyncio.sleep(2)

    async def _send_update(self, pkl):
        await self._client.send_message(pkl, WORKER_RESULT)

    async def _receive_forecast_request(self, data):
        if self._busy:
            await self._send_refused()
        else:
            try:
                print(f'Received forecast request for serie: "{data.get("serie_name")}"')
                await self._update_busy(True)
                worker_loop = asyncio.new_event_loop()
                wrapper = data.get('wrapper')
                wrapper = AnalyserWrapper(wrapper.get('_analyser_model'), wrapper.get('_model_type'),
                                          wrapper.get('_model_arguments'))
                self._worker_thread = Thread(target=start_analysing, args=(
                    worker_loop, self._result_queue, self._max_job_duration, data.get("serie_name"),
                    wrapper,
                    self._config['siridb']['user'],
                    self._config['siridb']['password'],
                    self._config['siridb']['database'],
                    self._config['siridb']['host'],
                    self._config['siridb']['port'],))
                # Start the thread
                self._worker_thread.start()
            except Exception as e:
                print("WOW: ", e)
                print(traceback.print_exc())

    async def _add_handshake_data(self):
        return {'busy': self._busy}

    async def start_worker(self):
        await self._client.setup(cbs={
            # UPDATE_SERIES: self._handle_update_series
            FORECAST_SERIE: self._receive_forecast_request
        },
            handshake_cb=self._add_handshake_data)
        self._client_run_task = self._loop.create_task(self._client.run())
        self._updater_task = self._loop.create_task(self._check_for_update())

    async def shutdown(self):
        self._running = False
        await self._send_shutdown()
        await self._client.close()
