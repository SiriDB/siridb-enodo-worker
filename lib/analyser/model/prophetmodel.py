import datetime
import time

import pandas as pd

from fbprophet import Prophet
from lib.analyser.model.base import Model


class ProphetModel(Model):

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
        # self._dataset['ds'] = pd.to_datetime(self._dataset['ds'], format="%Y-%m-%d %H:%M:%S")
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
        print("h1")
        if update or self.forecast_values is None:
            future = self._model.make_future_dataframe(periods=20)
            future.tail()

            print("h2")
            forecast = self._model.predict(future)
            # forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
            forecast.set_index('ds')

            print("h3")
            print(forecast)
            forecast['ds'] = pd.to_datetime(forecast['ds'], format="%Y-%m-%d %H:%M:%S")
            indexed_forecast_values = []
            for index, row in forecast.iterrows():
                indexed_forecast_values.append(
                    [int(time.mktime(datetime.datetime.strptime(str(row['ds']), "%Y-%m-%d %H:%M:%S").timetuple())),
                     row['yhat']])

                self.forecast_values = indexed_forecast_values
            return self.forecast_values
        else:
            return self.forecast_values
