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
                              'worker', self._config['enodo']['internal_security_token'],
                              heartbeat_interval=int(self._config['enodo']['heartbeat_interval']))
        self._client_run_task = None
        self._updater_task = None
        self._result_queue = Queue()
        self._busy = False
        self._started_last_job = None
        self._max_job_duration = self._config['enodo']['max_job_duration']
        self._worker_thread = None
        self._current_job = None
        self._running = True
        self._cancel_current_job = False

    async def _update_busy(self, busy, job_id=None):
        self._busy = busy
        self._current_job = job_id
        await self._client.send_message(busy, WORKER_UPDATE_BUSY)

    async def _send_refused(self):
        await self._client.send_message(None, WORKER_REFUSED)

    async def _send_shutdown(self):
        await self._client.send_message(None, CLIENT_SHUTDOWN)

    async def _check_for_update(self):
        while self._running:
            if not self._result_queue.empty():
                try:
                    result = self._result_queue.get()
                except Exception as e:
                    print(e)
                    pass
                else:
                    result['job_id'] = self._current_job
                    await self._send_update(result)
                    await self._update_busy(False)
            await asyncio.sleep(2)

    async def _send_update(self, pkl):
        await self._client.send_message(pkl, WORKER_JOB_RESULT)

    async def _receive_job(self, data):
        if self._busy:
            await self._send_refused()
        else:
            try:
                print(f'Received job request for serie: "{data.get("serie_name")}"')
                await self._update_busy(True, data.get('job_id'))
                worker_loop = asyncio.new_event_loop()
                wrapper = data.get('wrapper')
                wrapper = AnalyserWrapper(wrapper.get('_analyser_model'), wrapper.get('_model_type'),
                                          wrapper.get('_model_arguments'))
                self._worker_thread = Thread(target=start_analysing, args=(
                    worker_loop,
                    self._result_queue,
                    self._max_job_duration,
                    self._should_cancel,
                    self._send_job_cancelled,
                    data.get("serie_name"),
                    data.get("job_type"),
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

    async def _cancel_job(self, data):
        if self._busy:
            job_id = data.get('job_id')
            if job_id is not None and self._current_job == job_id:
                self._cancel_current_job = True

    async def _should_cancel(self):
        should = self._cancel_current_job
        if should:
            await self._job_cancelled()
        return should

    async def _job_cancelled(self):
        self._cancel_current_job = False

    async def _send_job_cancelled(self):
        await self._client.send_message({"job_id": self._current_job}, WORKER_JOB_CANCELLED)
        await self._update_busy(False)

    async def _add_handshake_data(self):
        return {'busy': self._busy}

    async def start_worker(self):
        await self._client.setup(cbs={
            WORKER_JOB: self._receive_job,
            WORKER_JOB_CANCEL: self._cancel_job
        },
            handshake_cb=self._add_handshake_data)
        self._client_run_task = self._loop.create_task(self._client.run())
        self._updater_task = self._loop.create_task(self._check_for_update())

    async def shutdown(self):
        self._running = False
        await self._send_shutdown()
        await self._client.close()
