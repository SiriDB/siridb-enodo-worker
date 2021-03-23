<p align="center"><img src="https://github.com/siridb/siridb-enodo-hub/raw/development/assets/logo_full.png" alt="Enodo"></p>

# Enodo

### Listener

The enode worker executes fitting and forecasting models/algorithms. The worker uses significant CPU and thus should be placed on a machine that has low CPU usage.
The worker can create different models (ARIMA/prophet/ffe) for series, train models with new data and calculate forecasts for a certain series.


## Getting started

To get the Enodo Listener setup you need to following the following steps:

### Locally

1. Install dependencies via `pip3 install -r requirements.txt`
2. Setup a .conf file file `python3 main.py --create-config` There will be made a `default.conf` next to the main.py.
3. Fill in the `default.conf` file
4. Call `python3 main.py --config=default.conf` to start the hub.
5. You can also setup the config by environment variables. These names are identical to those in the default.conf file, except all uppercase.