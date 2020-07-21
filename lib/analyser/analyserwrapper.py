ARIMA_MODEL = 'ARIMA'
PROPHET_MODEL = 'prophet'
AR_MODEL = 'AR'
MA_MODEL = 'MA'

MODEL_PARAMETERS = {ARIMA_MODEL: ['m', 'd', 'D'], PROPHET_MODEL: [], AR_MODEL: [], MA_MODEL: []}


class AnalyserWrapper:
    model_name = None
    model_arguments = None

    def __init__(self, model_name, arguments):
        self.model_name = model_name
        self.model_arguments = arguments
