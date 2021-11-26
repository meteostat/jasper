"""
DWD global monthly data import routine

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
from sys import argv
from ftplib import FTP
import hashlib
import pandas as pd
from routines import Routine
from routines.schema import normals_global

# Configuration
ENDPOINT = 'http://opendata.dwd.de/climate_environment/CDC/observations_global/CLIMAT/multi_annual/'
START = 1961
END = 1990


# DataFrame config
PARAMETERS = [
    {
        'dir': 'precipitation_total/',
        'name': 'prcp',
        'stubnames': {
            'Station': 'station',
            'Jan': 'prcp1',
            'Feb': 'prcp2',
            'Mrz': 'prcp3',
            'Apr': 'prcp4',
            'Mai': 'prcp5',
            'Jun': 'prcp6',
            'Jul': 'prcp7',
            'Aug': 'prcp8',
            'Sep': 'prcp9',
            'Okt': 'prcp10',
            'Nov': 'prcp11',
            'Dez': 'prcp12'
        }
    },
    {
        'dir': 'air_temperature_mean/',
        'name': 'tavg',
        'stubnames': {
            'Station': 'station',
            'Jan': 'tavg1',
            'Feb': 'tavg2',
            'Mrz': 'tavg3',
            'Apr': 'tavg4',
            'Mai': 'tavg5',
            'Jun': 'tavg6',
            'Jul': 'tavg7',
            'Aug': 'tavg8',
            'Sep': 'tavg9',
            'Okt': 'tavg10',
            'Nov': 'tavg11',
            'Dez': 'tavg12'
        }
    },
    {
        'dir': 'air_temperature_mean_of_daily_max/',
        'name': 'tmax',
        'stubnames': {
            'Station': 'station',
            'Jan': 'tmax1',
            'Feb': 'tmax2',
            'Mrz': 'tmax3',
            'Apr': 'tmax4',
            'Mai': 'tmax5',
            'Jun': 'tmax6',
            'Jul': 'tmax7',
            'Aug': 'tmax8',
            'Sep': 'tmax9',
            'Okt': 'tmax10',
            'Nov': 'tmax11',
            'Dez': 'tmax12'
        }
    },
    {
        'dir': 'air_temperature_mean_of_daily_min/',
        'name': 'tmin',
        'stubnames': {
            'Station': 'station',
            'Jan': 'tmin1',
            'Feb': 'tmin2',
            'Mrz': 'tmin3',
            'Apr': 'tmin4',
            'Mai': 'tmin5',
            'Jun': 'tmin6',
            'Jul': 'tmin7',
            'Aug': 'tmin8',
            'Sep': 'tmin9',
            'Okt': 'tmin10',
            'Nov': 'tmin11',
            'Dez': 'tmin12'
        }
    },
    {
        'dir': 'mean_sea_level_pressure/',
        'name': 'pres',
        'stubnames': {
            'Station': 'station',
            'Jan': 'pres1',
            'Feb': 'pres2',
            'Mrz': 'pres3',
            'Apr': 'pres4',
            'Mai': 'pres5',
            'Jun': 'pres6',
            'Jul': 'pres7',
            'Aug': 'pres8',
            'Sep': 'pres9',
            'Okt': 'pres10',
            'Nov': 'pres11',
            'Dez': 'pres12'
        }
    },
    {
        'dir': 'sunshine_duration/',
        'name': 'tsun',
        'stubnames': {
            'Station': 'station',
            'Jan': 'tsun1',
            'Feb': 'tsun2',
            'Mrz': 'tsun3',
            'Apr': 'tsun4',
            'Mai': 'tsun5',
            'Jun': 'tsun6',
            'Jul': 'tsun7',
            'Aug': 'tsun8',
            'Sep': 'tsun9',
            'Okt': 'tsun10',
            'Nov': 'tsun11',
            'Dez': 'tsun12'
        }
    }
]

# Create task
task = Routine('import.dwd.normals.global')

# DataFrame which holds all data
df_full = None

for parameter in PARAMETERS:

    # Read CSV
    df = pd.read_csv(f'{ENDPOINT}{parameter["dir"]}{START}_{END}.txt', ';', dtype={ 'Station': 'object' })

    # Rename columns
    df = df.rename(columns=parameter['stubnames'])

    # Translate from wide to long
    df = pd.wide_to_long(df, stubnames=parameter['name'], i='station', j='month')

    # Sunshine hours to minutes
    if parameter['name'] == 'tsun':
        df['tsun'] = df['tsun'] * 60

    # Append data to full DataFrame
    if df_full is None:
        df_full = df
    else:
        df_full = df_full.join(df)

# Add start & end year
df_full['start'] = START
df_full['end'] = END

# Write DataFrame into Meteostat database
task.write(df_full, normals_global)
