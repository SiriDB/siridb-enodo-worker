import datetime
import time

import pandas as pd
import numpy as np
from statsmodels.tsa.ar_model import AR

from lib.analyser.model.base import Model


class AutoRegressionModel(Model):

    def __init__(self, series_name, dataset, freq='2H'):
        """
        Start modelling a time serie
        :param series_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints
        :param m: the seasonality factor
        :param d: the de-rending differencing factor
        :param d_large: the de-seasonality differencing factor
        """
        super().__init__(series_name, dataset)
        self._model = None
        self._dataset = dataset

        self.forecast_values = None
        self.is_stationary = False
        self._dataset.columns = ['ds', 'y']
        # self._dataset['ds'] = pd.to_datetime(self._dataset['ds'], unit='s')

        self._dataset['datetime'] = pd.to_datetime(self._dataset['ds'], unit='s')
        self._dataset = self._dataset.set_index('datetime')
        self._dataset.drop(['ds'], axis=1, inplace=True)
        self._dataset.head()
        self._dataset = self._dataset.asfreq(freq=freq, method="pad")

    def create_model(self):
        series = pd.Series(self._dataset['y'], index=self._dataset.index)
        self._model = AR(series, missing='drop')
        self._model = self._model.fit()

    def do_forecast(self, update=False):
        """
        When a model is present, a set of forecasted future values can be generated.
        :param update:
        :return:
        """
        # freq = pd.Timedelta(self._find_frequency(self._dataset['ds'])).ceil('H')
        # periods = int(datetime.timedelta(days=7) / freq)
        # print(freq, periods)
        #
        # if periods < 20:
        #     periods = 20
        # l = 0/0

        if update or self.forecast_values is None:
            yhat = self._model.predict(len(self._dataset), len(self._dataset) + 200)
            # yhat = self._model.predict(start=1, end=5)
            indexed_forecast_values = []
            # values = yhat.to_frame()
            values = pd.DataFrame({'ds': yhat.index, 'yhat': yhat.values})
            for index, row in values.iterrows():
                indexed_forecast_values.append(
                    [int(time.mktime(
                        datetime.datetime.strptime(str(row['ds']), "%Y-%m-%d %H:%M:%S").timetuple())),
                     row['yhat']])
            self.forecast_values = indexed_forecast_values
            return self.forecast_values
        else:
            return self.forecast_values
