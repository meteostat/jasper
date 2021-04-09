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
from routines.schema import monthly_global

# Configuration
DWD_FTP_SERVER = 'opendata.dwd.de'
MODE = argv[1]
BASE_DIR = 'precipitation_total/' + MODE
STATIONS_PER_CYCLE = int(argv[2])


# DataFrame config
PARAMETERS = [
    {
        'dir': 'precipitation_total/' + MODE,
        'name': 'prcp',
        'stubnames': {
            'Jahr': 'year',
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
        'dir': 'air_temperature_mean/' + MODE,
        'name': 'tavg',
        'stubnames': {
            'Jahr': 'year',
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
        'dir': 'air_temperature_absolute_max/' + MODE,
        'name': 'tmax',
        'stubnames': {
            'Jahr': 'year',
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
        'dir': 'air_temperature_absolute_min/' + MODE,
        'name': 'tmin',
        'stubnames': {
            'Jahr': 'year',
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
        'dir': 'mean_sea_level_pressure/' + MODE,
        'name': 'pres',
        'stubnames': {
            'Jahr': 'year',
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
        'dir': 'sunshine_duration/' + MODE,
        'name': 'tsun',
        'stubnames': {
            'Jahr': 'year',
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

# Find file in directory
def find_file(ftp: object, path: str, needle: str):

    file = None

    try:

        ftp.cwd(
            '/climate_environment/CDC/observations_global/CLIMAT/monthly/qc/' +
            path)
        files = ftp.nlst()
        matching = [f for f in files if needle in f]
        file = matching[0]

    except BaseException:

        pass

    return file


# Create task
task = Routine('import.dwd.monthly.global')

# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd('/climate_environment/CDC/observations_global/CLIMAT/monthly/qc/' + BASE_DIR)

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

        # Get WMO ID
        wmo_id = str(
            station_file[-9:-4]) if MODE == 'recent' else str(station_file[-23:-18])
        station = pd.read_sql(f'SELECT `id` FROM `stations` WHERE `wmo` LIKE "{wmo_id}"', task.db).iloc[0][0]

        # DataFrame which holds data for one weather station
        df_station = None

        # Go through all parameters
        for parameter in PARAMETERS:

            try:

                remote_file = find_file(ftp, parameter['dir'], wmo_id)

                if remote_file is not None:

                    hash = hashlib.md5(remote_file.encode('utf-8')).hexdigest()
                    local_file = os.path.dirname(__file__) + os.sep + hash
                    ftp.retrbinary(
                        "RETR " + remote_file,
                        open(
                            local_file,
                            'wb').write)

                    # Convert raw data to DataFrame
                    df = pd.read_csv(local_file, ';')

                    # Remove local file
                    os.remove(local_file)

                    # Rename columns
                    df = df.rename(columns=parameter['stubnames'])

                    # Add weather station ID
                    df['station'] = station

                    # Translate from wide to long
                    df = pd.wide_to_long(df, stubnames=parameter['name'], i=['station', 'year'], j='month')

                    # Sunshine hours to minutes
                    if parameter['name'] == 'tsun':
                        df['tsun'] = df['tsun'] * 60

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
task.write(df_full, monthly_global)
