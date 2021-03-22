"""
NOAA ISD Lite import routine

Get hourly weather data for weather stations worldwide.

The code is licensed under the MIT license.
"""

import os
from sys import argv
from datetime import datetime
from io import BytesIO
from ftplib import FTP
import gzip
import pandas as pd
from routines import Routine
from routines.convert import ms_to_kmh, temp_dwpt_to_rhum
from routines.schema import hourly_global

# Configuration
MODE = argv[1]
STATIONS_PER_CYCLE = 1 if MODE == 'recent' else 4
USAF_WBAN_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '../../..',
        'resources')) + '/usaf_wban.csv'
CURRENT_YEAR = datetime.now().year

# Required columns
usecols = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10]

# Column names
NAMES = ['time', 'temp', 'dwpt', 'pres', 'wdir', 'wspd', 'prcp']

# Create new task
task = Routine('import.noaa.hourly.global')

# Get counter value
counter = task.get_var('station_counter_' + MODE)
skip = 0 if counter is None else int(counter)

# Get year
if MODE == 'historical':
    year = task.get_var('year')
    year = 1901 if year is None else int(year)

# Get ISD Lite stations
try:
    stations = pd.read_csv(
        USAF_WBAN_PATH,
        dtype='str',
        skiprows=skip,
        nrows=STATIONS_PER_CYCLE,
        names=[
            'id',
            'usaf',
            'wban'])
except pd.errors.EmptyDataError:
    stations = None
    pass

# Update counter
if stations is None or len(stations.index) < STATIONS_PER_CYCLE:
    # Reset counter
    task.set_var('station_counter_' + MODE, 0)
    # Reset year
    if MODE == 'historical':
        if year >= CURRENT_YEAR - 2:
            task.set_var('year', 1901)
        else:
            task.set_var('year', year + 1)
    exit()
else:
    task.set_var('station_counter_' + MODE, skip + STATIONS_PER_CYCLE)

# Connect to NOAA FTP Server
ftp = FTP('ftp.ncdc.noaa.gov')
ftp.login()

# Get list of years
if MODE == 'recent':
    years = range(CURRENT_YEAR - 1, CURRENT_YEAR + 1)
else:
    years = range(year, year + 1)

# Import data for each weather station
for station in stations.to_dict(orient='records'):

    for year in years:

        try:

            ftp.cwd('/pub/data/noaa/isd-lite/' + str(year))

            filename = station["usaf"] + '-' + \
                station["wban"] + '-' + str(year) + '.gz'

            if filename in ftp.nlst():

                # Download file
                local_file = os.path.dirname(__file__) + os.sep + filename
                ftp.retrbinary(
                    "RETR " + filename,
                    open(
                        local_file,
                        'wb').write)

                # Unzip file
                file = gzip.open(local_file, 'rb')
                raw = file.read()
                file.close()

                # Remove .gz file
                os.remove(local_file)

                df = pd.read_fwf(
                    BytesIO(raw),
                    parse_dates={
                        'time': [
                            0,
                            1,
                            2,
                            3]},
                    na_values=-
                    9999,
                    header=None,
                    usecols=usecols)

                # Rename columns
                df.columns = NAMES

                # Adapt columns
                df['temp'] = df['temp'].div(10)
                df['dwpt'] = df['dwpt'].div(10)
                df['pres'] = df['pres'].div(10)
                df['wspd'] = df['wspd'].div(10).apply(ms_to_kmh)
                df['prcp'] = df['prcp'].div(10)

                # Calculate humidity data
                df['rhum'] = df.apply(
                    lambda row: temp_dwpt_to_rhum(row), axis=1)

                # Drop dew point column
                df = df.drop('dwpt', axis=1)

                # Add station column
                df['station'] = station['id']

                # Set index
                df = df.set_index(['station', 'time'])

                # Round decimals
                df = df.round(1)

                # Write data into Meteostat database
                task.write(df, hourly_global)

        except BaseException:

            pass

# Quit FTP connection
ftp.quit()
