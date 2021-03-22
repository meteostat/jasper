"""
DWD POI import routine

Get hourly synop data for selected weather stations in Europe.

The code is licensed under the MIT license.
"""

import os
import pandas as pd
from routines import Routine
from routines.schema import hourly_synop

# Configuration
STATIONS_PER_CYCLE = 10
POI_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '../../..',
        'resources')) + '/poi.csv'

# Column which should be imported
usecols = [0, 1, 9, 21, 22, 23, 33, 35, 36, 37, 40, 41]

# Columns which should be parsed as dates
parse_dates = {
    'time': [0, 1]
}

# Column names
names = {
    'Temperatur (2m)': 'temp',
    'Windgeschwindigkeit': 'wspd',
    'Windboen (letzte Stunde)': 'wpgt',
    'Niederschlag (letzte Stunde)': 'prcp',
    'Relative Feuchte': 'rhum',
    'Windrichtung': 'wdir',
    'Druck (auf Meereshoehe)': 'pres',
    'Sonnenscheindauer (letzte Stunde)': 'tsun',
    'aktuelles Wetter': 'coco',
    'Schneehoehe': 'snow'
}

# Create new task
task = Routine('import.dwd.hourly.synop')

# Map DWD codes to Meteostat condicodes


def get_condicode(code: str):
    """ Check docs/dwd_poi_codes.pdf for more information """

    condicodes = {
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '8': 8,
        '9': 9,
        '10': 10,
        '11': 11,
        '12': 12,
        '13': 13,
        '14': 14,
        '15': 15,
        '16': 16,
        '17': 24,
        '18': 17,
        '19': 18,
        '20': 19,
        '21': 20,
        '22': 21,
        '23': 22,
        '24': 19,
        '25': 20,
        '26': 23,
        '27': 25,
        '28': 26,
        '29': 25,
        '30': 26,
        '31': 27,
    }

    return condicodes.get(str(code), None)


# Get counter value
counter = task.get_var('station_counter')
skip = 0 if counter is None else int(counter)

# Get POI stations
try:
    stations = pd.read_csv(
        POI_PATH,
        dtype='str',
        skiprows=skip,
        nrows=STATIONS_PER_CYCLE,
        names=['id'])
except pd.errors.EmptyDataError:
    stations = None
    pass

# Update counter
if stations is None or len(stations.index) < STATIONS_PER_CYCLE:
    task.set_var('station_counter', 0)
    exit()
else:
    task.set_var('station_counter', skip + STATIONS_PER_CYCLE)

# DataFrame which holds all data
df_full = None

# Import data for each weather station
for station in stations.to_dict(orient='records'):

    try:

        # Read CSV data from DWD server
        url = f"http://opendata.dwd.de/weather/weather_reports/poi/{station['id']}-BEOB.csv"
        df = pd.read_csv(
            url,
            ';',
            skiprows=2,
            na_values='---',
            usecols=usecols,
            decimal=',',
            parse_dates=parse_dates)

        # Rename columns
        df = df.rename(columns=names)

        # Snow cm -> mm
        df['snow'] = df['snow'].multiply(10)

        # Change coco
        df['coco'] = df['coco'].apply(get_condicode)

        # Add station column
        df['station'] = station['id']

        # Set index
        df = df.set_index(['station', 'time'])

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except BaseException:

        pass

# Write DataFrame into Meteostat database
task.write(df_full, hourly_synop)
