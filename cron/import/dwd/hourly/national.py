"""
DWD national hourly data import routine

Get hourly data for weather stations in Germany.

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
import sys
from typing import Union
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime
import hashlib
import pandas as pd
from meteor import Meteor, run
from meteor.convert import ms_to_kmh
from meteor.schema import hourly_national


class Task(Meteor):
    """
    Import hourly (national) data from DWD
    """

    MODE = sys.argv[1]  # 'recent' or 'historical'
    name = f'import.dwd.hourly.national.{MODE}'  # Task name
    # dev_mode = True  # Running in dev mode?

    # DWD open data server
    DWD_FTP_SERVER = 'opendata.dwd.de'
    # Base directory on DWD server
    BASE_DIR = f'precipitation/{MODE}'
    # Number of weather stations per execution
    STATIONS_PER_CYCLE = int(sys.argv[2])
    # Parameter config
    PARAMETERS = [
        {
            'dir': f'precipitation/{MODE}',
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
            'dir': f'air_temperature/{MODE}',
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
            'dir': f'wind/{MODE}',
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
            'dir': f'pressure/{MODE}',
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
            'dir': f'sun/{MODE}',
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
    # FTP server connection
    ftp: Union[FTP, None] = None
    # The task's counter
    counter = 0
    # How many lines to skip
    skip = 3
    # List of weather stations
    stations = []
    # DataFrame which holds all data
    df_full: Union[pd.DataFrame, None] = None

    @staticmethod
    def dateparser(value):
        """
        Custom Pandas date parser
        """
        return datetime.strptime(value, '%Y%m%d%H')

    def _find_file(self, path: str, needle: str):
        """
        Find file in directory
        """
        match = None

        try:
            self.ftp.cwd(
                '/climate_environment/CDC/observations_germany/climate/hourly/' +
                path)
            files = self.ftp.nlst()
            matching = [f for f in files if needle in f]
            match = matching[0]
        except BaseException:
            pass

        return match

    def _connect_dwd(self) -> None:
        """
        Connect to DWD FTP server
        """
        self.ftp = FTP(self.DWD_FTP_SERVER)
        self.ftp.login()
        self.ftp.cwd(
            f'/climate_environment/CDC/observations_germany/climate/hourly/{self.BASE_DIR}'
        )

    def _get_files(self) -> None:
        """
        Get all files in directory
        """
        try:
            endpos = self.STATIONS_PER_CYCLE + self.skip
            self.stations = self.ftp.nlst()[self.skip:endpos]
        except BaseException:
            pass

    def _update_counter(self) -> None:
        """
        Update counter value
        """
        if len(self.stations) < self.STATIONS_PER_CYCLE:
            self.set_var('station_counter', 0)
            sys.exit()
        else:
            self.set_var(
                'station_counter',
                self.counter +
                self.STATIONS_PER_CYCLE)

    # pylint: disable=too-many-branches,too-many-nested-blocks,too-many-locals
    # Refactor in a future version
    def main(self) -> None:
        """
        Main script & entry point
        """
        # Connect to FTP server
        self._connect_dwd()

        # Get counter value
        self.counter = self.get_var('station_counter', 0, int)
        self.skip = 3 if self.counter == 0 else 3 + self.counter

        # Get all files in directory
        self._get_files()

        # Update counter
        self._update_counter()

        for station_file in self.stations:
            try:
                # Get national weather station ID
                national_id = str(
                    station_file[-13:-8]) if self.MODE == 'recent' else str(station_file[-32:-27]
                                                                            )
                station = pd.read_sql(f'''
                    SELECT `id` FROM `stations` WHERE `national_id` LIKE "{national_id}"
                ''', self.db).iloc[0][0]

                # DataFrame which holds data for one weather station
                df_station = None

                # Go through all parameters
                for parameter in self.PARAMETERS:
                    try:
                        remote_file = self._find_file(
                            parameter['dir'], national_id)

                        if remote_file is not None:
                            file_hash = hashlib.md5(
                                remote_file.encode('utf-8')).hexdigest()
                            local_file = os.path.dirname(
                                __file__) + os.sep + file_hash
                            with open(local_file, 'wb') as f:
                                self.ftp.retrbinary(
                                    "RETR " + remote_file,
                                    f.write
                                )

                            # Unzip file
                            with ZipFile(local_file, 'r') as zipped:
                                filelist = zipped.namelist()
                                raw = None
                                for file in filelist:
                                    if file[:7] == 'produkt':
                                        with zipped.open(file, 'r') as reader:
                                            raw = BytesIO(reader.read())

                            # Remove ZIP file
                            os.remove(local_file)

                            # Convert raw data to DataFrame
                            df: pd.DataFrame = pd.read_csv(
                                raw,
                                sep=';',
                                date_parser=Task.dateparser,
                                na_values='-999',
                                usecols=parameter['usecols'],
                                parse_dates=parameter['parse_dates']
                            )

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
                if self.df_full is None:
                    self.df_full = df_station
                else:
                    self.df_full = self.df_full.append(df_station)

            except BaseException:
                pass

        # Write DataFrame into Meteostat database
        self.persist(self.df_full, hourly_national)


# Run task
run(Task)
