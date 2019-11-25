import asyncio
import configparser
import datetime
import traceback

from queue import Queue

from enodo import EnodoModel
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
        self._current_job_started_at = None
        self._models = []

    async def _update_busy(self, busy, job_id=None):
        self._busy = busy
        self._current_job = job_id
        self._current_job_started_at = datetime.datetime.now() if busy else None
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
            if self._busy and (
                    datetime.datetime.now() - self._current_job_started_at).total_seconds() >= int(
                self._max_job_duration):
                await self._cancel_job()
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
                from func_timeout import StoppableThread
                self._worker_thread = StoppableThread(target=start_analysing, args=(
                    worker_loop,
                    self._result_queue,
                    data.get("serie_name"),
                    data.get("job_type"),
                    data.get("job_data"),
                    wrapper,
                    self._config['siridb']['user'],
                    self._config['siridb']['password'],
                    self._config['siridb']['database'],
                    self._config['siridb']['host'],
                    self._config['siridb']['port'],))
                # Start the thread
                self._worker_thread.daemon = True
                self._worker_thread.start()
            except Exception as e:
                print("WOW: ", e)
                print(traceback.print_exc())

    async def _cancel_job(self):
        try:
            self._worker_thread.stop(Exception, 2.0)
        except Exception as e:
            print('e')
        finally:
            await self._send_job_cancelled()

    async def _receive_to_cancel_job(self, data):
        job_id = data.get('job_id')
        if job_id is self._current_job:
            await self._cancel_job()

    async def _send_job_cancelled(self):
        await self._client.send_message({"job_id": self._current_job}, WORKER_JOB_CANCELLED)
        await self._update_busy(False)

    async def _add_handshake_data(self):
        return {'busy': self._busy,
                'models': [await EnodoModel.to_dict(model) for model in self._models]}

    async def start_worker(self):
        prophet_model = EnodoModel('prophet', {}, supports_forecasting=True, supports_anomaly_detection=True)
        arima_model = EnodoModel('arima', {'m': True, 'd': True, 'D': True}, supports_forecasting=True,
                                 supports_anomaly_detection=True)
        self._models.append(prophet_model)
        self._models.append(arima_model)

        await self._client.setup(cbs={
            WORKER_JOB: self._receive_job,
            WORKER_JOB_CANCEL: self._receive_to_cancel_job
        },
            handshake_cb=self._add_handshake_data)
        self._client_run_task = self._loop.create_task(self._client.run())
        self._updater_task = self._loop.create_task(self._check_for_update())

    async def shutdown(self):
        self._running = False
        await self._send_shutdown()
        await self._client.close()
