
import numpy as np
import pandas as pd

async def basic_series_analysis(data_points):
    slope = await _get_series_slope(data_points)

    return {
        "trend": slope
    }

async def _get_series_slope(data):
    try:
        coeffs = np.polyfit(data.index.values, list(data), 1)
        slope = coeffs[-2]
    except Exception as _:
        return None
    return float(slope)