import datetime

import pandas as pd

from fbprophet import Prophet
from lib.analyser.model.base import Model


class MovingAverageModel(Model):

    def __init__(self, serie_name, dataset):
        """
        Start modelling a time serie
        :param serie_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints
        :param m: the seasonality factor
        :param d: the de-rending differencing factor
        :param d_large: the de-seasonality differencing factor
        """
        super().__init__(serie_name, dataset)
        self._model = None
        self._dataset = dataset

        self.forecast_values = None
        self.is_stationary = False
        self._dataset.columns = ['ds', 'y']
        self._dataset['ds'] = pd.to_datetime(self._dataset['ds'], unit='s')
        print(self._dataset)

    def create_model(self):
        self._model = Prophet()
        self._model.fit(self._dataset)

    def do_forecast(self, update=False):
        """
        When is a model is present, a set of forecasted future values can be generated.
        :param update:
        :return:
        """
        freq = pd.Timedelta(self._find_frequency(self._dataset['ds'])).ceil('H')
        periods = int(datetime.timedelta(days=7) / freq)
        print(freq, periods)

        if periods < 20:
            periods = 20

        if update or self.forecast_values is None:
            pass
        else:
            return self.forecast_values
