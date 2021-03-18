import asyncio
import logging

import pandas as pd

# from analyserwrapper import *
from lib.analyser.model.autoregressionmodel import AutoRegressionModel
from lib.analyser.model.movingaveragemodel import MovingAverageModel
from lib.analyser.model.prophetmodel import ProphetModel
from lib.analyser.model.ffemodel import FastFourierExtrapolationModel
from lib.analyser.baseanalysis import basic_series_analysis
from lib.siridb.siridb import SiriDB

from enodo.jobs import JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_STATIC_RULES



class Analyser:
    _analyser_queue = None
    _busy = None
    _siridb_client = None
    _shutdown = None
    _current_future = None

    def __init__(self, queue, siridb_user, siridb_password, siridb_db, siridb_host, siridb_port):
        self._siridb_client = SiriDB(siridb_user, siridb_password, siridb_db, siridb_host, siridb_port)
        self._analyser_queue = queue

    async def execute_job(self, job_data):
        self._analyser_queue.put({'name': 'asassa', 'error': 'Job type not implemented'})
        return
        series_name = job_data.get("series_name")
        job_type = job_data.get("job_type")
        series_data = await self._siridb_client.query_series_data(series_name)
        dataset = pd.DataFrame(series_data[series_name])

        if job_type == JOB_TYPE_BASE_SERIES_ANALYSIS:
            await self._analyse_series(series_name, dataset)
        elif job_type == JOB_TYPE_STATIC_RULES:
            parameters = job_data.get('series_config').get(JOB_TYPE_BASE_SERIES_ANALYSIS).get('model_params').get('static_rules')
            await self._check_static_rules(series_name, dataset, parameters)
        else:
            job_config = job_data.get('series_config').get('job_config').get(job_type)
            model = job_config.get('model')
            parameters = job_config.get('model_params')
            try:
                if model == 'prophet':
                    analysis = ProphetModel(series_name, dataset, 100)
                elif model =='ffe':
                    analysis = FastFourierExtrapolationModel(series_name, series_data[series_name], parameters)
                else:
                    raise Exception()
            except Exception as e:
                error = str(e)
                self._analyser_queue.put({'name': series_name, 'error': error})
            else:
                if job_type == JOB_TYPE_FORECAST_SERIES:
                    await self._forcast_series(series_name, analysis, job_data)
                elif job_type == JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES:
                    await self._detect_anomalies(series_name, analysis, job_data)
                else:
                    self._analyser_queue.put({'name': series_name, 'error': 'Job type not implemented'})

    async def _analyse_series(self, series_name, dataset):
        points = dataset[0]
        characteristics = await basic_series_analysis(points)

        self._analyser_queue.put(
                    {'name': series_name, 'job_type': JOB_TYPE_BASE_SERIES_ANALYSIS, 'characteristics': characteristics})

    async def _check_static_rules(self, series_name, dataset, static_rules):
        min_value = static_rules.get('min')
        max_value = static_rules.get('max')
        last_n_points = int(static_rules.get('last_n_points'))

        rows_to_check = dataset.tail(last_n_points)
        failed_checks = {}

        if min_value is not None:
            data_min = rows_to_check[1].min()
            if data_min < min_value:
                failed_checks['min'] = f"Found value lower than min value. ({data_min} < {min_value})"
        if max_value is not None:
            data_max = rows_to_check[1].max()
            if data_max > max_value:
                failed_checks['max'] = f"Found value higher than max value. ({data_max} > {max_value})"

        self._analyser_queue.put(
                {'name': series_name, 'job_type': JOB_TYPE_STATIC_RULES, 'failed_checks': failed_checks})

    async def _forcast_series(self, series_name, analysis_model, job_data):
        """
        Collects data for starting an analysis of a specific time serie
        :param series_name:
        :return:
        """
        error = None
        forecast_values = []
        try:
            analysis_model.create_model()
            forecast_values = analysis_model.do_forecast()
        except Exception as e:
            error = str(e)
            logging.error('Error while making and executing forcast model')
            logging.debug(f'Correspondig error: {str(e)}')
        finally:
            if error is not None:
                self._analyser_queue.put({'name': series_name, 'job_type': JOB_TYPE_FORECAST_SERIES, 'error': error})
            else:
                self._analyser_queue.put(
                    {'name': series_name, 'job_type': JOB_TYPE_FORECAST_SERIES, 'points': forecast_values})

    async def _detect_anomalies(self, series_name, analysis_model, job_data):
        since = job_data.get('series_config').get('model_params').get('points_since')
        if since is None:
            self._analyser_queue.put(
                {'name': series_name, 'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
                 'error': 'Missing data `points_since` for anomaly detection'})
            return
        error = None
        anomalies_timestamps = []
        try:
            analysis_model.create_model()
            anomalies_timestamps = analysis_model.find_anomalies(since)
        except Exception as e:
            error = str(e)
            logging.error('Error while making and executing anomaly detection model')
            logging.debug(f'Correspondig error: {str(e)}')
        finally:
            if error is not None:
                self._analyser_queue.put({'name': series_name, 'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, 'error': error})
            else:
                self._analyser_queue.put(
                    {'name': series_name, 'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, 'anomalies': anomalies_timestamps})


async def _save_start_with_timeout(loop, queue, job_data,
                                   siridb_user, siridb_password,
                                   siridb_dbname, siridb_host,
                                   siridb_port):
    try:
        asyncio.set_event_loop(loop)
        analyser = Analyser(queue, siridb_user, siridb_password, siridb_dbname, siridb_host,
                            siridb_port)
        await analyser.execute_job(job_data)
    except Exception as e:
        logging.error('Error while executing Analyzer')
        logging.debug(f'Correspondig error: {str(e)}')


def start_analysing(loop, queue, job_data,
                    siridb_user, siridb_password,
                    siridb_dbname, siridb_host, siridb_port):
    """Switch to new event loop and run forever"""
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _save_start_with_timeout(loop, queue, job_data,
                                     siridb_user, siridb_password,
                                     siridb_dbname, siridb_host,
                                     siridb_port))
        loop.stop()
    except Exception as e:
        exit()
