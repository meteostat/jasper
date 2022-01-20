"""
NOAA ISD Lite import routine

Get hourly weather data for weather stations worldwide.

The code is licensed under the MIT license.
"""

import os
import sys
from typing import Union
from datetime import datetime
from io import BytesIO
from ftplib import FTP
import gzip
import pandas as pd
from meteor import Meteor, run
from meteor.convert import ms_to_kmh, temp_dwpt_to_rhum
from meteor.schema import hourly_global


class Task(Meteor):
    """
    Import hourly (global) data from NOAA
    """

    MODE = sys.argv[1]
    name = f'import.noaa.hourly.global.{MODE}'  # Task name
    # dev_mode = True  # Running in dev mode?

    STATIONS_PER_CYCLE = 1 if MODE == 'recent' else 4
    USAF_WBAN_PATH = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '../../../..',
            'resources'
        )
    ) + '/usaf_wban.csv'
    CURRENT_YEAR = datetime.now().year
    # Column ranges
    COLSPECS = [
        (0, 4),
        (5, 7),
        (8, 10),
        (11, 13),
        (13, 19),
        (19, 25),
        (25, 31),
        (31, 37),
        (37, 43),
        (49, 55)
    ]
    # Column names
    NAMES = ['time', 'temp', 'dwpt', 'pres', 'wdir', 'wspd', 'prcp']
    stations: Union[pd.DataFrame, None] = None

    # pylint: disable=too-many-branches,too-many-statements
    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get counter value
        skip = self.get_var('station_counter', 0, int)

        # Get year
        if self.MODE == 'historical':
            year = self.get_var('year', 1901, int)

        # Get ISD Lite stations
        try:
            self.stations = pd.read_csv(
                self.USAF_WBAN_PATH,
                dtype='str',
                skiprows=skip,
                nrows=self.STATIONS_PER_CYCLE,
                names=[
                    'id',
                    'usaf',
                    'wban'
                ]
            )
        except pd.errors.EmptyDataError:
            pass

        # Update counter
        if self.stations is None or len(
                self.stations.index) < self.STATIONS_PER_CYCLE:
            # Reset counter
            self.set_var('station_counter', 0)
            # Reset year
            if self.MODE == 'historical':
                if year >= self.CURRENT_YEAR - 2:
                    self.set_var('year', 1901)
                else:
                    self.set_var('year', year + 1)
            sys.exit()
        else:
            self.set_var('station_counter', skip + self.STATIONS_PER_CYCLE)

        # Connect to NOAA FTP Server
        ftp = FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        # Get list of years
        if self.MODE == 'recent':
            years = range(self.CURRENT_YEAR - 1, self.CURRENT_YEAR + 1)
        else:
            years = range(year, year + 1)

        # Import data for each weather station
        # pylint: disable=no-member
        for station in self.stations.to_dict(orient='records'):
            for year in years:
                try:
                    ftp.cwd('/pub/data/noaa/isd-lite/' + str(year))

                    filename = station["usaf"] + '-' + \
                        station["wban"] + '-' + str(year) + '.gz'

                    if filename in ftp.nlst():
                        # Download file
                        local_file = os.path.dirname(
                            __file__) + os.sep + filename
                        with open(local_file, 'wb') as f:
                            ftp.retrbinary(
                                "RETR " + filename,
                                f.write
                            )

                        # Unzip file
                        file = gzip.open(local_file, 'rb')
                        raw = file.read()
                        file.close()

                        # Remove .gz file
                        os.remove(local_file)

                        df = pd.read_fwf(
                            BytesIO(raw),
                            parse_dates={
                                'time': [0, 1, 2, 3]
                            },
                            na_values=['-9999', -9999],
                            header=None,
                            colspecs=self.COLSPECS
                        )

                        # Rename columns
                        df.columns = self.NAMES

                        # Adapt columns
                        df['temp'] = df['temp'].div(10)
                        df['dwpt'] = df['dwpt'].div(10)
                        df['pres'] = df['pres'].div(10)
                        df['wspd'] = df['wspd'].div(10).apply(ms_to_kmh)
                        df['prcp'] = df['prcp'].div(10)

                        # Calculate humidity data
                        # pylint: disable=unnecessary-lambda
                        df['rhum'] = df.apply(
                            lambda row: temp_dwpt_to_rhum(row),
                            axis=1
                        )

                        # Drop dew point column
                        # pylint: disable=no-member
                        df = df.drop('dwpt', axis=1)

                        # Add station column
                        df['station'] = station['id']

                        # Set index
                        df = df.set_index(['station', 'time'])

                        # Round decimals
                        df = df.round(1)

                        # Write data into Meteostat database
                        self.persist(df, hourly_global)

                except BaseException:
                    pass

        # Quit FTP connection
        ftp.quit()


run(Task)
