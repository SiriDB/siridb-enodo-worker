import asyncio
import pandas as pd

from lib.analyser.analyserwrapper import *
from lib.analyser.model.arimamodel import ARIMAModel
# from lib.analyser.model.prophetmodel import ProphetModel
from lib.analyser.model.autoregressionmodel import AutoRegressionModel
from lib.analyser.model.movingaveragemodel import MovingAverageModel
from lib.analyser.model.prophetmodel import ProphetModel
from lib.exceptions.analyserexception import AnalyserException
from lib.siridb.siridb import SiriDB


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

    async def analyse_serie(self, serie_name, model, parameters):
        """
        Collects data for starting an analysis of a specific time serie
        :param serie_name:
        :return:
        """
        # serie_data = await cls._siridb_client.query_serie_data(serie_name, "mean (4d)")
        serie_data = await self._siridb_client.query_serie_data(serie_name)
        dataset = pd.DataFrame(serie_data[serie_name])
        error = None
        forecast_values = []
        try:
            if model is ARIMA_MODEL:
                analysis = ARIMAModel(serie_name, dataset, m=parameters.get('m', 12),
                                      d=parameters.get('d', None),
                                      d_large=parameters.get('D', None))
            elif model is PROPHET_MODEL:
                analysis = ProphetModel(serie_name, dataset)
            elif model is AR_MODEL:
                analysis = AutoRegressionModel(serie_name, dataset)
            elif model is MA_MODEL:
                analysis = MovingAverageModel(serie_name, dataset)
            else:
                raise Exception()

            analysis.create_model()
            forecast_values = analysis.do_forecast()
        except Exception as e:
            error = str(e)
        finally:
            print(error)
            if error is not None:
                self._analyser_queue.put({'name': serie_name, 'error': error})
            else:
                self._analyser_queue.put({'name': serie_name, 'points': forecast_values})


async def _save_start_with_timeout(loop, queue, max_job_duration, serie_name, analyser_wrapper, siridb_user,
                                   siridb_password,
                                   siridb_dbname, siridb_host,
                                   siridb_port):
    try:
        asyncio.set_event_loop(loop)
        analyser = Analyser(queue, analyser_wrapper, siridb_user, siridb_password, siridb_dbname, siridb_host,
                            siridb_port)
        await asyncio.wait_for(
            analyser.analyse_serie(serie_name, analyser_wrapper._model_type, analyser_wrapper._model_arguments),
            timeout=int(max_job_duration))
    except asyncio.TimeoutError:
        print('timeout!')


def start_analysing(loop, queue, max_job_duration, serie_name, analyser_wrapper, siridb_user, siridb_password,
                    siridb_dbname, siridb_host,
                    siridb_port):
    """Switch to new event loop and run forever"""
    try:
        loop.run_until_complete(
            _save_start_with_timeout(loop, queue, max_job_duration, serie_name, analyser_wrapper, siridb_user,
                                     siridb_password,
                                     siridb_dbname, siridb_host,
                                     siridb_port))
        loop.stop()
    except Exception as e:
        print(e)
        import traceback
        print(traceback.print_exc())
        exit()
