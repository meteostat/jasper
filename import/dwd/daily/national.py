"""
DWD national daily data import routine

Get daily data for weather stations in Germany.

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
from sys import argv
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime
import hashlib
import pandas as pd
from routines import Routine
from routines.convert import pres_to_msl, ms_to_kmh
from routines.schema import daily_national

# Configuration
DWD_FTP_SERVER = 'opendata.dwd.de'
MODE = argv[1]
STATIONS_PER_CYCLE = int(argv[2])
def DATEPARSER(x): return datetime.strptime(x, '%Y%m%d')


USECOLS = [1, 3, 4, 6, 8, 9, 12, 13, 14, 15, 16]

PARSE_DATES = {
    'time': [0]
}

NAMES = {
    'FX': 'wpgt',
    'FM': 'wspd',
    'RSK': 'prcp',
    'SDK': 'tsun',
    'SHK_TAG': 'snow',
    'PM': 'pres',
    'TMK': 'tavg',
    'UPM': 'rhum',
    'TXK': 'tmax',
    'TNK': 'tmin'
}

# Create task
task = Routine('import.dwd.daily.national')

# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd('/climate_environment/CDC/observations_germany/climate/daily/kl/' + MODE)

# Get counter value
counter = task.get_var(f'station_counter_{MODE}')
counter = int(counter) if counter is not None else 0
skip = 3 if counter is None else 3 + counter

# Get all files in directory
try:
    endpos = STATIONS_PER_CYCLE + skip
    files = ftp.nlst()[skip:endpos]
except BaseException:
    files = None
    pass

# Update counter
if files is None or len(files) < STATIONS_PER_CYCLE:
    task.set_var(f'station_counter_{MODE}', 0)
    exit()
else:
    task.set_var(f'station_counter_{MODE}', counter + STATIONS_PER_CYCLE)

# DataFrame which holds all data
df_full = None

for remote_file in files:

    try:

        # Get national weather station ID
        national_id = str(
            remote_file[-13:-8]) if MODE == 'recent' else str(remote_file[-32:-27])
        station_df = pd.read_sql(f'SELECT `id`, `altitude` FROM `stations` WHERE `national_id` LIKE "{national_id}"')
        station = station_df.iloc[0][0]
        altitude = station_df.iloc[0][1]

        hash = hashlib.md5(remote_file.encode('utf-8')).hexdigest()
        local_file = os.path.dirname(__file__) + os.sep + hash
        ftp.retrbinary("RETR " + remote_file, open(local_file, 'wb').write)

        # Unzip file
        zipped = ZipFile(local_file, 'r')
        filelist = zipped.namelist()
        raw = None
        for file in filelist:
            if file[:7] == 'produkt':
                raw = BytesIO(zipped.open(file, 'r').read())

        # Remove ZIP file
        os.remove(local_file)

        # Convert raw data to DataFrame
        df = pd.read_csv(raw, ';', date_parser=DATEPARSER, na_values=[
                         '-999', -999], usecols=USECOLS, parse_dates=PARSE_DATES)

        # Rename columns
        df = df.rename(columns=lambda x: x.strip())
        df = df.rename(columns=NAMES)

        # Convert PRES to MSL
        df['pres'] = df.apply(lambda row: pres_to_msl(row, altitude), axis=1)
        df['wpgt'] = df['wpgt'].apply(ms_to_kmh)
        df['wspd'] = df['wspd'].apply(ms_to_kmh)
        df['tsun'] = df['tsun'] * 60
        df['snow'] = df['snow'] * 10

        # Add weather station ID
        df['station'] = station

        # Set index
        df = df.set_index(['station', 'time'])

        # Round decimals
        df = df.round(1)

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except BaseException:

        pass

# Write DataFrame into Meteostat database
task.write(df_full, daily_national)
