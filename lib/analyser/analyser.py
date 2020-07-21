import asyncio

import pandas as pd

from lib.analyser.analyserwrapper import *
from lib.analyser.model.arimamodel import ARIMAModel
from lib.analyser.model.autoregressionmodel import AutoRegressionModel
from lib.analyser.model.movingaveragemodel import MovingAverageModel
from lib.analyser.model.prophetmodel import ProphetModel
from lib.analyser.baseanalysis import basic_series_analysis
from lib.siridb.siridb import SiriDB

JOB_TYPE_FORECAST_SERIE = 1
JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE = 2
JOB_TYPE_BASE_SERIE_ANALYSES = 3

JOB_TYPES = [JOB_TYPE_BASE_SERIE_ANALYSES, JOB_TYPE_FORECAST_SERIE, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE]



class Analyser:
    _analyser_queue = None
    _busy = None
    _siridb_client = None
    _shutdown = None
    _current_future = None

    def __init__(self, queue, wrapper, siridb_user, siridb_password, siridb_db, siridb_host, siridb_port):
        self._siridb_client = SiriDB(siridb_user, siridb_password, siridb_db, siridb_host, siridb_port)
        self._wrapper = wrapper
        self._analyser_queue = queue

    async def execute_job(self, job_type, job_data, serie_name, model, parameters):
        serie_data = await self._siridb_client.query_serie_data(serie_name)
        dataset = pd.DataFrame(serie_data[serie_name])

        if job_type not in JOB_TYPES:
            self._analyser_queue.put({'name': serie_name, 'error': 'Unknown job type'})
            return

        if job_type is JOB_TYPE_BASE_SERIE_ANALYSES:
                await self._analyse_serie(serie_name, dataset)
        else:
            try:
                if model == ARIMA_MODEL:
                    analysis = ARIMAModel(serie_name, dataset, m=parameters.get('m', 12),
                                        d=parameters.get('d', None),
                                        d_large=parameters.get('D', None))
                elif model == PROPHET_MODEL:
                    analysis = ProphetModel(serie_name, dataset, parameters.get('forecast_points_in_future'))
                elif model == AR_MODEL:
                    analysis = AutoRegressionModel(serie_name, dataset)
                elif model == MA_MODEL:
                    analysis = MovingAverageModel(serie_name, dataset)
                else:
                    raise Exception()
            except Exception as e:
                error = str(e)
                self._analyser_queue.put({'name': serie_name, 'error': error})
            else:
                if job_type not in JOB_TYPES:
                    self._analyser_queue.put({'name': serie_name, 'error': 'Unknown job type'})
                elif job_type is JOB_TYPE_FORECAST_SERIE:
                    await self._forcast_serie(serie_name, analysis, job_data)
                elif job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                    await self._detect_anomalies(serie_name, analysis, job_data)
                else:
                    self._analyser_queue.put({'name': serie_name, 'error': 'Job type not implemented'})

    async def _analyse_serie(self, serie_name, dataset):
        points = dataset[0]
        characteristics = await basic_series_analysis(points)

        self._analyser_queue.put(
                    {'name': serie_name, 'job_type': JOB_TYPE_BASE_SERIE_ANALYSES, 'characteristics': characteristics})



    async def _forcast_serie(self, serie_name, analysis_model, job_data):
        """
        Collects data for starting an analysis of a specific time serie
        :param serie_name:
        :return:
        """
        print("START ANALYSE")
        error = None
        forecast_values = []
        try:
            analysis_model.create_model()
            forecast_values = analysis_model.do_forecast()
        except Exception as e:
            error = str(e)
        finally:
            print(error)
            if error is not None:
                self._analyser_queue.put({'name': serie_name, 'job_type': JOB_TYPE_FORECAST_SERIE, 'error': error})
            else:
                self._analyser_queue.put(
                    {'name': serie_name, 'job_type': JOB_TYPE_FORECAST_SERIE, 'points': forecast_values})

    async def _detect_anomalies(self, serie_name, analysis_model, job_data):
        since = job_data.get('points_since')
        if since is None:
            self._analyser_queue.put(
                {'name': serie_name, 'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE,
                 'error': 'Missing data `points_since` for anomaly detection'})
            return
        error = None
        anomalies_timestamps = []
        try:
            analysis_model.create_model()
            anomalies_timestamps = analysis_model.find_anomalies(since)
        except Exception as e:
            error = str(e)
        finally:
            print(error)
            if error is not None:
                self._analyser_queue.put({'name': serie_name, 'job_type': WORKER_JOB_DETECT_ANOMALIES, 'error': error})
            else:
                self._analyser_queue.put(
                    {'name': serie_name, 'job_type': WORKER_JOB_DETECT_ANOMALIES, 'anomalies': anomalies_timestamps})


async def _save_start_with_timeout(loop, queue, job_type, job_data,
                                   serie_name, analyser_wrapper, siridb_user,
                                   siridb_password,
                                   siridb_dbname, siridb_host,
                                   siridb_port):
    try:
        asyncio.set_event_loop(loop)
        analyser = Analyser(queue, analyser_wrapper, siridb_user, siridb_password, siridb_dbname, siridb_host,
                            siridb_port)
        await analyser.execute_job(job_type, job_data, serie_name, analyser_wrapper.model_name,
                                   analyser_wrapper.model_arguments)
    except Exception as e:
        print(e)
        import traceback
        traceback.print_exc()


def start_analysing(loop, queue, serie_name, job_type, job_data,
                    analyser_wrapper,
                    siridb_user, siridb_password,
                    siridb_dbname, siridb_host, siridb_port):
    """Switch to new event loop and run forever"""
    print("ENTERING THREAD")
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _save_start_with_timeout(loop, queue, job_type, job_data,
                                     serie_name, analyser_wrapper, siridb_user,
                                     siridb_password,
                                     siridb_dbname, siridb_host,
                                     siridb_port))
        loop.stop()
    except Exception as e:
        print(e)
        import traceback
        print(traceback.print_exc())
        exit()
