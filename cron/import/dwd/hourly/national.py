"""
DWD national hourly data import routine

Get hourly data for weather stations in Germany.

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
from routines.convert import ms_to_kmh
from routines.schema import hourly_national

# Configuration
DWD_FTP_SERVER = 'opendata.dwd.de'
MODE = argv[1]
BASE_DIR = 'precipitation/' + MODE
STATIONS_PER_CYCLE = int(argv[2])
def DATEPARSER(x): return datetime.strptime(x, '%Y%m%d%H')


# DataFrame config
PARAMETERS = [
    {
        'dir': 'precipitation/' + MODE,
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'R1': 'prcp'
        },
        'convert': {}
    },
    {
        'dir': 'air_temperature/' + MODE,
        'usecols': [1, 3, 4],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'TT_TU': 'temp',
            'RF_TU': 'rhum'
        },
        'convert': {}
    },
    {
        'dir': 'wind/' + MODE,
        'usecols': [1, 3, 4],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'F': 'wspd',
            'D': 'wdir'
        },
        'convert': {
            'wspd': ms_to_kmh
        }
    },
    {
        'dir': 'pressure/' + MODE,
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'P': 'pres'
        },
        'convert': {}
    },
    {
        'dir': 'sun/' + MODE,
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'SD_SO': 'tsun'
        },
        'convert': {}
    }
]

# Find file in directory
def find_file(ftp: object, path: str, needle: str):

    file = None

    try:

        ftp.cwd(
            '/climate_environment/CDC/observations_germany/climate/hourly/' +
            path)
        files = ftp.nlst()
        matching = [f for f in files if needle in f]
        file = matching[0]

    except BaseException:

        pass

    return file


# Create task
task = Routine('import.dwd.hourly.national')

# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd('/climate_environment/CDC/observations_germany/climate/hourly/' + BASE_DIR)

# Get counter value
counter = task.get_var(f'station_counter_{MODE}')
counter = int(counter) if counter is not None else 0
skip = 3 if counter is None else 3 + counter

# Get all files in directory
try:
    endpos = STATIONS_PER_CYCLE + skip
    stations = ftp.nlst()[skip:endpos]
except BaseException:
    stations = None
    pass

# Update counter
if stations is None or len(stations) < STATIONS_PER_CYCLE:
    task.set_var(f'station_counter_{MODE}', 0)
    exit()
else:
    task.set_var(f'station_counter_{MODE}', counter + STATIONS_PER_CYCLE)

# DataFrame which holds all data
df_full = None

for station_file in stations:

    try:

        # Get national weather station ID
        national_id = str(
            station_file[-13:-8]) if MODE == 'recent' else str(station_file[-32:-27])
        station = pd.read_sql(f'SELECT `id` FROM `stations` WHERE `national_id` LIKE "{national_id}"', task.db).iloc[0][0]

        # DataFrame which holds data for one weather station
        df_station = None

        # Go through all parameters
        for parameter in PARAMETERS:

            try:

                remote_file = find_file(ftp, parameter['dir'], national_id)

                if remote_file is not None:

                    hash = hashlib.md5(remote_file.encode('utf-8')).hexdigest()
                    local_file = os.path.dirname(__file__) + os.sep + hash
                    ftp.retrbinary(
                        "RETR " + remote_file,
                        open(
                            local_file,
                            'wb').write)

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
                    df = pd.read_csv(
                        raw,
                        ';',
                        date_parser=DATEPARSER,
                        na_values='-999',
                        usecols=parameter['usecols'],
                        parse_dates=parameter['parse_dates'])

                    # Rename columns
                    df = df.rename(columns=lambda x: x.strip())
                    df = df.rename(columns=parameter['names'])

                    # Convert column data
                    for col, func in parameter['convert'].items():
                        df[col] = df[col].apply(func)

                    # Add weather station ID
                    df['station'] = station

                    # Set index
                    df = df.set_index(['station', 'time'])

                    # Round decimals
                    df = df.round(1)

                    # Append data to full DataFrame
                    if df_station is None:
                        df_station = df
                    else:
                        df_station = df_station.join(df)

            except BaseException:

                pass

        # Append data to full DataFrame
        if df_full is None:
            df_full = df_station
        else:
            df_full = df_full.append(df_station)

    except BaseException:

        pass

# Write DataFrame into Meteostat database
task.write(df_full, hourly_national)
