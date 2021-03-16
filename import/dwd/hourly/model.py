"""
DWD MOSMIX import routine

Get hourly MOSMIX model data for selected weather stations.

The code is licensed under the MIT license.
"""

import os
import re
import math
from datetime import datetime
from urllib import request
from zipfile import ZipFile
from lxml import etree
import pandas as pd
from routines import Routine
from routines.schema import hourly_model

# Configuration
STATIONS_PER_CYCLE = 6
MOSMIX_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../../..', 'resources')) + '/mosmix.csv'

# Convert Kelvin to Celsius
def kelvin_to_celsius(value):
    return value - 273.15

# Convert m/s to km/h
def ms_to_kmh(value):
    return value * 3.6

# Get relative humidity from temperature and dew point
def get_humidity(row: dict):
    return 100 * (math.exp((17.625 * row['dwpt']) / (243.04 + row['dwpt'])) / math.exp((17.625 * row['temp']) / (243.04 + row['temp'])))

# Map DWD codes to Meteostat condicodes
def get_condicode(code: str):

    condicodes = {
        '0': 1,
        '1': 2,
        '2': 3,
        '3': 4,
        '45': 5,
        '49': 5,
        '61': 7,
        '63': 8,
        '65': 9,
        '51': 7,
        '53': 8,
        '55': 9,
        '68': 12,
        '69': 13,
        '71': 14,
        '73': 15,
        '75': 16,
        '80': 17,
        '81': 18,
        '82': 18,
        '83': 19,
        '84': 20,
        '85': 21,
        '86': 22,
        '66': 10,
        '67': 11,
        '56': 10,
        '57': 11,
        '95': 25
    }

    return condicodes.get(str(code), None)

# Create new task
task = Routine('import.dwd.hourly.model')

# Get counter value
counter = task.get_var('station_counter')
skip = 0 if counter is None else int(counter)

# Get MOSMIX stations
try:
    stations = pd.read_csv(MOSMIX_PATH, dtype='str', skiprows=skip, nrows=STATIONS_PER_CYCLE, names=['id', 'mosmix'])
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

        # Load KMZ data from DWD server
        url = f"https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/{station['mosmix']}/kml/MOSMIX_L_LATEST_{station['mosmix']}.kmz"
        filename = os.path.dirname( __file__ ) + os.sep + station['id'] + '.kmz'
        request.urlretrieve(url, filename)

        # KMZ -> KML
        kmz = ZipFile(filename, 'r')
        kml = kmz.open(ZipFile.infolist(kmz)[0].filename, 'r').read()

        # Remove KMZ file
        os.remove(filename)

        # Parse KML
        tree = etree.fromstring(kml)

        # Skip stale forecasts
        issue_time = datetime.strptime(tree.xpath('//kml:kml/kml:Document/kml:ExtendedData/dwd:ProductDefinition/dwd:IssueTime', namespaces=tree.nsmap)[0].text, '%Y-%m-%dT%H:%M:%S.%fZ')
        if (datetime.now() - issue_time).total_seconds() > 25200:
            continue

        # Collect all time steps
        timesteps = []
        for step in tree.xpath('//kml:kml/kml:Document/kml:ExtendedData/dwd:ProductDefinition/dwd:ForecastTimeSteps/dwd:TimeStep', namespaces=tree.nsmap):
            timesteps.append(step.text)

        """ Collect weather data """
        data = {
            'time': timesteps,
            'pres': [],
            'temp': [],
            'dwpt': [],
            'wdir': [],
            'wspd': [],
            'wpgt': [],
            'coco': [],
            'prcp': []
        }
        placemark = tree.xpath('//kml:kml/kml:Document/kml:Placemark', namespaces=tree.nsmap)[0]

        # Pressure
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="PPPP"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['pres'].append(float(value) / 100 if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Air temperature
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="TTT"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['temp'].append(kelvin_to_celsius(float(value)) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Dew point
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="Td"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['dwpt'].append(kelvin_to_celsius(float(value)) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Wind direction
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="DD"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['wdir'].append(int(float(value)) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Wind speed
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="FF"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['wspd'].append(ms_to_kmh(float(value)) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Peak wind gust
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="FX1"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['wpgt'].append(ms_to_kmh(float(value)) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Weather condition
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="ww"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['coco'].append(get_condicode(int(float(value))) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Precipitation
        for value in re.sub(r'/\s+/', ' ', placemark.xpath('kml:ExtendedData/dwd:Forecast[@dwd:elementName="RR1c"]/dwd:value', namespaces=tree.nsmap)[0].text).strip().split():
            data['prcp'].append(float(value) if value.lstrip('-').replace('.','',1).isdigit() else None)

        # Convert data dict to DataFrame
        df = pd.DataFrame.from_dict(data)

        # Convert time strings to datetime
        df['time'] = pd.to_datetime(df['time'])

        # Calculate humidity data
        df['rhum'] = df.apply(lambda row: get_humidity(row), axis=1)

        # Drop dew point column
        df = df.drop('dwpt', axis=1)

        # Add station column
        df['station'] = station['id']

        # Set index
        df = df.set_index(['station', 'time'])

        # Round decimals
        df = df.round(1)

        # Remove tz awareness
        df = df.tz_convert(None, level='time')

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except:

        pass

# Write DataFrame into Meteostat database
task.write(df_full, hourly_model)
