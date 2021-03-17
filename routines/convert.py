import math

# Convert Kelvin to Celsius
def kelvin_to_celsius(value):
    return value - 273.15 if value is not None else None

# Convert m/s to km/h
def ms_to_kmh(value):
    return value * 3.6 if value is not None else None

# Get relative humidity from temperature and dew point
def temp_dwpt_to_rhum(row: dict):
    return 100 * (math.exp((17.625 * row['dwpt']) / (243.04 + row['dwpt'])) / math.exp((17.625 * row['temp']) / (243.04 + row['temp']))) if row['temp'] is not None and row['dwpt'] is not None else None
