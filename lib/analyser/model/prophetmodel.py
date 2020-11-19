import datetime
import time

import pandas as pd

from fbprophet import Prophet
from lib.analyser.model.base import Model


class ProphetModel(Model):

    def __init__(self, series_name, dataset, periods=None):
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
        self._raw_dataset = dataset
        self._dataset = dataset
        self._periods = periods

        self.forecast_values = None
        self.is_stationary = False
        self._dataset.columns = ['ds', 'y']
        self._dataset['ds'] = pd.to_datetime(self._dataset['ds'], unit='s')

        # remove outliers
        self._dataset = self._remove_outlier_in_df(self._dataset, 'y')
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
        if self._periods is None:
            periods = int(datetime.timedelta(days=7) / freq)
            if periods < 20:
                periods = 20
            self._periods = periods
        if update or self.forecast_values is None:
            future = self._model.make_future_dataframe(periods=self._periods, freq=freq, include_history=True)
            future.tail()

            forecast = self._model.predict(future)
            forecast.set_index('ds')

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

    def _predict_dateframe(self, dataframe):
        m = Prophet(daily_seasonality=False, yearly_seasonality=False, weekly_seasonality=False,
                    seasonality_mode='multiplicative',
                    interval_width=0.99,
                    changepoint_range=0.8)
        m = m.fit(dataframe)
        forecast = m.predict(dataframe)
        forecast['fact'] = dataframe['y'].reset_index(drop=True)
        return forecast

    def find_anomalies(self, points_since):
        forecast = self._predict_dateframe(self._raw_dataset)

        forecasted = forecast[['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()

        forecasted['anomaly'] = 0
        forecasted.loc[forecasted['fact'] > forecasted['yhat_upper'], 'anomaly'] = 1
        forecasted.loc[forecasted['fact'] < forecasted['yhat_lower'], 'anomaly'] = -1

        # anomaly importances
        forecasted['importance'] = 2
        forecasted.loc[forecasted['anomaly'] == 1, 'importance'] = \
            (forecasted['fact'] - forecasted['yhat_upper']) / forecast['fact']
        forecasted.loc[forecasted['anomaly'] == -1, 'importance'] = \
            (forecasted['yhat_lower'] - forecasted['fact']) / forecast['fact']

        anomalies = forecasted[forecasted.anomaly != 0]

        indexed_anomalies_values = []
        for index, row in anomalies.iterrows():
            df_unix_sec = int(row['ds'].timestamp())
            if df_unix_sec >= points_since:
                indexed_anomalies_values.append(
                    [df_unix_sec, row['fact']])

        return indexed_anomalies_values
