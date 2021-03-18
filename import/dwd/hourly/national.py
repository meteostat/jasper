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
PARAMETER_NAME = argv[1]
PARAMETER_MODE = argv[2]
PARAMETER_PATH = argv[3]
STATIONS_PER_CYCLE = argv[4]
DATEPARSER = lambda x: datetime.strptime(x, '%Y%m%d%H')

# DataFrame config
DF_CONFIG = {
    'PRCP': {
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'R1': 'prcp'
        },
        'convert': {}
    },
    'TEMP': {
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
    'WIND': {
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
    'PRES': {
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'P': 'pres'
        },
        'convert': {}
    },
    'TSUN': {
        'usecols': [1, 3],
        'parse_dates': {
            'time': [0]
        },
        'names': {
            'SD_SO': 'tsun'
        },
        'convert': {}
    }
}

# Create task
task = Routine('import.dwd.hourly.national')

# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd('/climate_environment/CDC/observations_germany/climate/hourly/' + PARAMETER_PATH)

# Get counter value
counter = task.get_var(f'station_counter_{PARAMETER_NAME}_{PARAMETER_MODE}')
skip = 3 if counter is None else int(counter)

# Get all files in directory
try:
    endpos = STATIONS_PER_CYCLE + skip
    files = ftp.nlst()[skip:endpos]
except:
    files = None
    pass

# Update counter
if files is None or len(files) < STATIONS_PER_CYCLE:
    task.set_var(f'station_counter_{PARAMETER_NAME}_{PARAMETER_MODE}', 3)
    exit()
else:
    task.set_var(f'station_counter_{PARAMETER_NAME}_{PARAMETER_MODE}', skip + STATIONS_PER_CYCLE)

# DataFrame which holds all data
df_full = None

for file in files:

    try:

        # Download file
        national_id = file[-13:-8]
        hash = hashlib.md5(file.encode('utf-8')).hexdigest()
        filename = os.path.dirname( __file__ ) + os.sep + hash
        ftp.retrbinary("RETR " + file, open(filename, 'wb').write)

        # Unzip file
        zipped = ZipFile(filename, 'r')
        filelist = zipped.namelist()
        raw = None
        for element in filelist:
            if element[:7] == 'produkt':
                raw = BytesIO(zipped.open(element, 'r').read())

        # Remove ZIP file
        os.remove(filename)

        # Convert raw data to DataFrame
        df = pd.read_csv(raw, ';', date_parser=DATEPARSER, na_values='-999', usecols=DF_CONFIG[PARAMETER_NAME]['usecols'], parse_dates=DF_CONFIG[PARAMETER_NAME]['parse_dates'])

        # Rename columns
        df = df.rename(columns=lambda x: x.strip())
        df = df.rename(columns=DF_CONFIG[PARAMETER_NAME]['names'])

        # Convert column data
        for col, func in DF_CONFIG[PARAMETER_NAME]['convert'].items():
            df[col] = df[col].apply(func)

        # Add weather station ID
        station = task.read(f"SELECT `id` FROM `stations` WHERE `national_id` LIKE '{national_id}'")
        df['station'] = station.iloc[0][0]

        # Set index
        df = df.set_index(['station', 'time'])

        # Round decimals
        df = df.round(1)

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except:

        pass

# Write DataFrame into Meteostat database
task.write(df_full, hourly_national)
