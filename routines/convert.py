import math
from numpy import isnan

# Convert Kelvin to Celsius


def kelvin_to_celsius(value):
    return value - 273.15 if value is not None else None

# Convert m/s to km/h


def ms_to_kmh(value):
    return value * 3.6 if value is not None else None

# Get relative humidity from temperature and dew point


def temp_dwpt_to_rhum(row: dict):
    return 100 * (math.exp((17.625 * row['dwpt']) / (243.04 + row['dwpt'])) / math.exp((17.625 * row['temp']) / (
        243.04 + row['temp']))) if row['temp'] is not None and row['dwpt'] is not None else None


def pres_to_msl(row: dict, altitude: int = None, temp: str = 'tavg'):
    try:
        return None if isnan(row['pres']) or isnan(row[temp]) or isnan(altitude) or altitude is None or row['pres'] == - \
            999 else round(row['pres'] * math.pow((1 - ((0.0065 * altitude) / (row[temp] + 0.0065 * altitude + 273.15))), -5.257), 1)
    except BaseException:
        return None
