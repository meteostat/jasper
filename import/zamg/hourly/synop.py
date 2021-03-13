"""
ZAMG hourly synop import routine

Get hourly synop data for selected weather stations in Austria.

The code is licensed under the MIT license.
"""

import pandas as pd
from routines import Routine
from routines.schema import hourly_synop

task = Routine('import.zamg.hourly.synop')

# Configuration
parse_dates = {
    'time': [1, 2]
}

usecols = [0, 3, 4, 5, 7, 8, 9, 11, 12, 13, 15]

names = {
    'Station': 'station',
    'T °C': 'temp',
    'RF %': 'rhum',
    'WR °': 'wdir',
    'WG km/h': 'wspd',
    'WSG km/h': 'wpgt',
    'N l/m²': 'prcp',
    'LDred hPa': 'pres',
    'SO %': 'tsun'
}

# Read CSV data
df = pd.read_csv('http://www.zamg.ac.at/ogd/', ';', parse_dates=parse_dates, usecols=usecols, decimal=',')

# Rename columns
df = df.rename(columns=names)

# Set index
df = df.set_index(['station', 'time'])

# Convert time zone to UTC
df = df.tz_localize('Europe/Vienna', level='time')
df = df.tz_convert(None, level='time')

# Convert sunshine from percent to minutes
df['tsun'] = round(60 * (df['tsun'] / 100))

# Write DataFrame into Meteostat database
task.write(df, hourly_synop)
