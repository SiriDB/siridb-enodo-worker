import os
import pickle

from lib.analyser.analyserwrapper import AnalyserWrapper, ARIMA_MODEL

# wrapper = AnalyserWrapper(None, ARIMA_MODEL, {'d': 1, 'D': 1, 'm': 12})
#
# pkl = pickle.dumps(wrapper)

# f = open(os.path.join("/tmp/hubtest/", "wrapper.pkl"), "wb")
# f.write(pkl)
# f.close()

# exit()

f = open(os.path.join("/tmp/hubtest/", "wrapper.pkl"), "rb")
data = f.read()
f.close()

unpkl = pickle.loads(data)

print(isinstance(unpkl, AnalyserWrapper))

print(unpkl._model_arguments)