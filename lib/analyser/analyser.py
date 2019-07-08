import asyncio
import pandas as pd

from lib.analyser.model.arimamodel import ARIMAModel
# from lib.analyser.model.prophetmodel import ProphetModel
from lib.exceptions.analyserexception import AnalyserException
from lib.siridb.siridb import SiriDB


class Analyser:
    _analyser_queue = None
    _busy = None
    _siridb_client = None
    _shutdown = None
    _current_future = None

    def __init__(self, queue, siridb_user, siridb_password, siridb_db, siridb_host, siridb_port):
        self._siridb_client = SiriDB(siridb_user, siridb_password, siridb_db, siridb_host, siridb_port)
        self._analyser_queue = queue

    @classmethod
    async def analyse_serie(self, serie_name, model, parameters):
        """
        Collects data for starting an analysis of a specific time serie
        :param serie_name:
        :return:
        """
        # serie_data = await cls._siridb_client.query_serie_data(serie_name, "mean (4d)")
        serie_data = await self._siridb_client.query_serie_data(serie_name)
        dataset = pd.DataFrame(serie_data[serie_name])
        try:
            analysis = ARIMAModel(serie_name, dataset, m=parameters.get('m', 12),
                                  d=parameters.get('d', None),
                                  d_large=parameters.get('D', None))
            # analysis = ProphetModel(serie_name, dataset)
            analysis.create_model()
            print(analysis.do_forecast())
        except AnalyserException:
            print("error")
            pass
        except Exception as e:
            print(e)
            import traceback
            traceback.print_exc()
        else:
            pkl = analysis.pickle()
            self._analyser_queue.put(pkl)


def start_analyser_worker(loop, queue, serie_name, config):
    """Switch to new event loop and run forever"""
    try:
        asyncio.set_event_loop(loop)
        analyser = Analyser(queue, config['siridb']['user'], config['siridb']['password'], config['siridb']['dbname'],
                            config['siridb']['host'], config['siridb']['port'])
        loop.run_until_complete(analyser.analyse_serie(serie_name))
    except Exception as e:
        print(e)
        exit()
