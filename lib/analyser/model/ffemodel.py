import datetime
import time
import pandas as pd

import numpy as np
from numpy import fft
from lib.analyser.model.base import Model


class FastFourierExtrapolationModel(Model):

    def __init__(self, series_name, dataset, model_params):
        """
        Start modelling a time serie
        :param series_name: name of the serie
        :param dataset: default siridb list of pairs
        """
        super().__init__(series_name, dataset, initialize=False)
        self._model = None
        self._raw_dataset = dataset
        self._dataset = dataset
        self._model_params = model_params

    def create_model(self):
        pass

    def do_forecast(self):
        """
        When is a model is present, a set of forecasted future values can be generated.
        :param update:
        :return:
        """
        n_predict = self._model_params.get('n_predict', 100)
        return self._fourier_extrapolation(self._dataset, n_predict, is_forecast=True)

    def find_anomalies(self, points_since):
        sensitivity = self._model_params.get('anomaly_detection_sensitivity', 2)
        fe = self._fourier_extrapolation(self._dataset, 0, is_forecast=False)

        if len(fe) != len(self._dataset):
            raise Exception("Unequal length between ts data and extrapolation")
        
        average_difference = 0
        start_index_to_analyse = None
        
        for i in range(len(self._dataset)):
            average_difference += abs(self._dataset[i][1] - fe[i][1]) / len(self._dataset)
            if start_index_to_analyse is None and fe[i][0] >= points_since:
                start_index_to_analyse = i

        max_difference_from_fe = sensitivity * average_difference
        anomalies = []

        for i in range(start_index_to_analyse, len(self._dataset)):
            if abs(self._dataset[i][1] - fe[i][1]) > max_difference_from_fe:
                anomalies.append(self._dataset[i])

        return anomalies

    def _fourier_extrapolation(self, list_ts_values, n_predict, is_forecast=False):
        if not is_forecast:
            n_predict = 0
        else:
            self._fourier_remove_outliers(list_ts_values)

        x = [r[1] for r in list_ts_values]
        n = len(x)
        n_harm = 10                     # number of harmonics in model
        t = np.arange(0, n)
        p = np.polyfit(t, x, 1)         # find linear trend in x
        x_notrend = x - p[0] * t        # detrended x
        x_freqdom = fft.fft(x_notrend)  # detrended x in frequency domain
        f = fft.fftfreq(n)              # frequencies
        indexes = list(range(n))
        # sort indexes by frequency, lower -> higher
        indexes.sort(key = lambda i: np.absolute(f[i]))
    
        t = np.arange(0 if not is_forecast else n, n + n_predict)
        restored_sig = np.zeros(t.size)
        for i in indexes[:1 + n_harm * 2]:
            ampli = np.absolute(x_freqdom[i]) / n   # amplitude
            phase = np.angle(x_freqdom[i])          # phase
            restored_sig += ampli * np.cos(2 * np.pi * f[i] * t + phase)
        
        fe_values = restored_sig + p[0] * t

        fe_ts_v = []
        original_data_set_length = len(list_ts_values)
        last_timestamp = None
        mean_interval = 0

        if not is_forecast:
            for i in range(len(fe_values)):
                if i < len(list_ts_values):
                    if last_timestamp is not None:
                        mean_interval += int((list_ts_values[i][0] - last_timestamp) / original_data_set_length)
                    last_timestamp = list_ts_values[i][0]
                last_timestamp += mean_interval
                current_ts = list_ts_values[i][0] if i < len(list_ts_values) else last_timestamp
                fe_ts_v.append([current_ts, fe_values[i]])
        else:
            for i in range(len(list_ts_values)):
                if last_timestamp is not None:
                    mean_interval += int((list_ts_values[i][0] - last_timestamp) / original_data_set_length)
                last_timestamp = list_ts_values[i][0]

            last_timestamp = list_ts_values[-1][0]
            for i in range(len(fe_values)):
                last_timestamp = last_timestamp + mean_interval
                fe_ts_v.append([last_timestamp, fe_values[i]])

        return fe_ts_v

    def _fourier_remove_outliers(self, data, sensitivity=2):
        fe = self._fourier_extrapolation(data, 0, is_forecast=False)

        if len(fe) != len(data):
            raise Exception("Unequal length between ts data and extrapolation")
        
        average_difference = 0
        start_index_to_analyse = None
        
        for i in range(len(data)):
            average_difference += abs(data[i][1] - fe[i][1]) / len(data)
            if start_index_to_analyse is None:
                start_index_to_analyse = i

        max_difference_from_fe = sensitivity * average_difference

        for i in range(start_index_to_analyse, len(data)):
            diff = abs(data[i][1] - fe[i][1])
            if diff > max_difference_from_fe:
                if fe[i][1] > data[i][1]:
                    data[i][1] += diff - max_difference_from_fe
                else:
                    data[i][1] -= diff - max_difference_from_fe