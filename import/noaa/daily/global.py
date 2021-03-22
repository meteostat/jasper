"""
NOAA GHCND import routine

Get daily weather data for weather stations worldwide.

The code is licensed under the MIT license.
"""

import os
import pandas as pd
from numpy import nan
from routines import Routine
from routines import ghcnd
from routines.convert import ms_to_kmh
from routines.schema import daily_global

# Configuration
STATIONS_PER_CYCLE = 1
GHCN_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '../../..',
        'resources')) + '/ghcn.csv'

# Column names
names = {
    'MM/DD/YYYY': 'time',
    'TMAX': 'tmax',
    'TMIN': 'tmin',
    'TAVG': 'tavg',
    'PRCP': 'prcp',
    'SNWD': 'snow',
    'AWDR': 'wdir',
    'AWND': 'wspd',
    'TSUN': 'tsun',
    'WSFG': 'wpgt'
}

# Create new task
task = Routine('import.noaa.daily.global')

# Get counter value
counter = task.get_var('station_counter')
skip = 0 if counter is None else int(counter)

# Get GHCN stations
try:
    stations = pd.read_csv(
        GHCN_PATH,
        dtype='str',
        skiprows=skip,
        nrows=STATIONS_PER_CYCLE,
        names=[
            'id',
            'ghcn'])
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

# Connect to NOAA FTP Server
ftp = ghcnd.connect_to_ftp()

# Import data for each weather station
for station in stations.to_dict(orient='records'):

    try:

        df = ghcnd.dly_to_df(ftp, station['ghcn'])

        # Filter relevant columns
        required_columns = [
            'TMAX',
            'TMIN',
            'TAVG',
            'PRCP',
            'SNWD',
            'AWDR',
            'AWND',
            'TSUN',
            'WSFG']
        df = df.drop(
            columns=[
                col for col in df if col not in required_columns])

        # Add missing columns
        for col in required_columns:
            if col not in df.columns:
                df[col] = nan

        # Rename columns
        df = df.reset_index().rename(columns=names)

        # Adapt columns
        df['tavg'] = df['tavg'].div(10)
        df['tmin'] = df['tmin'].div(10)
        df['tmax'] = df['tmax'].div(10)
        df['prcp'] = df['prcp'].div(10)
        df['wspd'] = df['wspd'].div(10).apply(ms_to_kmh)
        df['wpgt'] = df['wpgt'].div(10).apply(ms_to_kmh)

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
task.write(df_full, daily_global)

# Quit FTP connection
ftp.quit()
