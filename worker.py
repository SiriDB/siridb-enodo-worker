import asyncio
import configparser
import datetime
import traceback

from queue import Queue
from threading import Thread

from version import VERSION
from enodo import EnodoModel
from enodo.client import Client
from enodo.protocol.package import *
from enodo.jobs import JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_STATIC_RULES
from enodo.protocol.packagedata import EnodoJobDataModel

from lib.analyser.analyser import start_analysing
from lib.config import EnodoConfigParser


class Worker:

    def __init__(self, loop, config_path):
        self._loop = loop
        self._config = EnodoConfigParser()
        self._config.read(config_path)
        self._client = Client(loop,
                                self._config['enodo']['hub_hostname'],
                                int(self._config['enodo']['hub_port']),
                                'worker', self._config['enodo']['internal_security_token'],
                                heartbeat_interval=int(self._config['enodo']['heartbeat_interval']),
                                identity_file_path=".enodo_id", client_version=VERSION)

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
        self._jobs_and_models = {}

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
        print("HALLLO", data)
        if self._busy:
            await self._send_refused()
        else:
            print("BOB")
            try:
                data = EnodoJobDataModel.unserialize(data)
            
                print(f'Received job request for series: "{data.get("series_name")}"')
            except Exception as e:
                print(e)
            print('bob3')
            await self._update_busy(True, data.get('job_id'))
            worker_loop = asyncio.new_event_loop()
            job_type = data.get('job_type')
            try:
                if job_type in [JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_STATIC_RULES]:
                    model_name = data.get("model_name")
                    if not await self._check_support_job_and_model(job_type, model_name):
                        await self._send_update(
                        {'error': 'Unsupported model for job_type', 'job_id': data.get('job_id'), 'name': data.get("series_name")})
                        await self._update_busy(False)
                        return
                else:
                    await self._send_update(
                        {'error': 'Unsupported job_type', 'job_id': data.get('job_id'), 'name': data.get("series_name")})
                    await self._update_busy(False)
                    return
            except Exception as e:
                print(e)
                import traceback
                traceback.print_exc()
            try:
                from func_timeout import StoppableThread
                self._worker_thread = Thread(target=start_analysing, args=(
                    worker_loop,
                    self._result_queue,
                    data,
                    self._config['siridb']['user'],
                    self._config['siridb']['password'],
                    self._config['siridb']['database'],
                    self._config['siridb']['host'],
                    self._config['siridb']['port'],))
                # Start the thread
                # self._worker_thread.daemon = True
                self._worker_thread.start()
            except Exception as e:
                print("WOW: ", e)
                print(traceback.print_exc())

    async def _check_support_job_and_model(self, job_type, model_name=None):
        if job_type in self._jobs_and_models.keys():
            if model_name is None:
                return True
            for model in self._jobs_and_models.get(job_type):
                if model_name == model.model_name:
                    return True
        return False

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
        serialized_jobs_and_models = {}
        for job in self._jobs_and_models:
            serialized_jobs_and_models[job] = [EnodoModel.to_dict(model) for model in self._jobs_and_models[job]]

        return {'busy': self._busy,
                'jobs_and_models': serialized_jobs_and_models}

    async def start_worker(self):
        # Declare model params
        prophet_model = EnodoModel('prophet', {})
        ffe_model = EnodoModel('ffe', {'points_since': True, 'sensitivity': True})
        static_rule_engine = EnodoModel('static_rule_engine', {})

        # init job models
        self._jobs_and_models[JOB_TYPE_FORECAST_SERIES] = list()
        self._jobs_and_models[JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES] = list()
        self._jobs_and_models[JOB_TYPE_BASE_SERIES_ANALYSIS] = list()
        self._jobs_and_models[JOB_TYPE_STATIC_RULES] = list()

        #insert models per job
        self._jobs_and_models[JOB_TYPE_BASE_SERIES_ANALYSIS].append(prophet_model)

        self._jobs_and_models[JOB_TYPE_FORECAST_SERIES].append(prophet_model)
        self._jobs_and_models[JOB_TYPE_FORECAST_SERIES].append(ffe_model)
        
        self._jobs_and_models[JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES].append(prophet_model)
        self._jobs_and_models[JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES].append(ffe_model)

        self._jobs_and_models[JOB_TYPE_STATIC_RULES].append(static_rule_engine)

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
