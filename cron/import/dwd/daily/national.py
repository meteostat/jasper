"""
DWD national daily data import task

Get daily data for weather stations in Germany.

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
import sys
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime
import hashlib
import pandas as pd
from meteor import Meteor, run
from meteor.convert import pres_to_msl, ms_to_kmh
from meteor.schema import daily_national


class Task(Meteor):
    """
    Import national daily data from DWD
    """

    MODE = sys.argv[1]  # 'recent' or 'historical'
    name = f'import.dwd.daily.national.{MODE}'  # Task name
    # dev_mode = True  # Running in dev mode?

    # DWD open data server
    DWD_FTP_SERVER = 'opendata.dwd.de'
    # Number of stations per execution
    STATIONS_PER_CYCLE = int(sys.argv[2])
    # CSV cols which should be read
    USECOLS = [1, 3, 4, 6, 8, 9, 12, 13, 14, 15, 16]
    # Which columns should be parsed as dates?
    PARSE_DATES = {
        'time': [0]
    }
    # Map col names to Meteostat parameter names
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
    # The task's counter
    counter = 0
    # How many lines to skip
    skip = 3
    # DWD FTP connection
    ftp: FTP = None
    # List of remote files
    files = []
    # DataFrame which holds all data
    df_full: pd.DataFrame = None

    @staticmethod
    def dateparser(value):
        """
        Custom Pandas date parser
        """
        return datetime.strptime(value, '%Y%m%d')

    def _connect_dwd(self) -> None:
        """
        Connect to DWD FTP server
        """
        self.ftp = FTP(self.DWD_FTP_SERVER)
        self.ftp.login()
        self.ftp.cwd(
            '/climate_environment/CDC/observations_germany/climate/daily/kl/' +
            self.MODE)

    def _get_files(self) -> None:
        """
        Get all files in directory
        """
        try:
            endpos = self.STATIONS_PER_CYCLE + self.skip
            self.files = self.ftp.nlst()[self.skip:endpos]
        except BaseException:
            pass

    def _update_counter(self) -> None:
        """
        Update counter value
        """
        if self.files is None or len(self.files) < self.STATIONS_PER_CYCLE:
            self.set_var('station_counter', 0)
            sys.exit()
        else:
            self.set_var(
                'station_counter',
                self.counter +
                self.STATIONS_PER_CYCLE)

    def main(self):
        """
        Main script & entry point
        """
        # Connect to DWD FTP server
        self._connect_dwd()

        # Get counter value
        self.counter = self.get_var('station_counter', 0, int)
        self.skip = 3 if self.counter == 0 else 3 + self.counter

        # Get files
        self._get_files()

        # Update counter
        self._update_counter()

        # Process each file
        for remote_file in self.files:
            try:
                # Get meta info for weather station
                national_id = str(
                    remote_file[-13:-8]) if self.MODE == 'recent' else str(remote_file[-32:-27])
                station_df = pd.read_sql(f'''
                    SELECT `id`, `altitude` FROM `stations` WHERE `national_id` LIKE "{national_id}"
                ''', self.db)
                station = station_df.iloc[0][0]
                altitude = station_df.iloc[0][1]

                # Get remote file
                file_hash = hashlib.md5(
                    remote_file.encode('utf-8')).hexdigest()
                local_file = os.path.dirname(__file__) + os.sep + file_hash
                # pylint: disable=consider-using-with
                self.ftp.retrbinary(
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
                df: pd.DataFrame = pd.read_csv(
                    raw,
                    sep=';',
                    date_parser=Task.dateparser,
                    na_values=['-999', -999],
                    usecols=self.USECOLS,
                    parse_dates=self.PARSE_DATES
                )

                # Rename columns
                df = df.rename(columns=lambda x: x.strip())
                df = df.rename(columns=self.NAMES)

                # Convert PRES to MSL
                df['pres'] = df.apply(
                    lambda row, alt=altitude: pres_to_msl(row, alt),
                    axis=1
                )
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
                if self.df_full is None:
                    self.df_full = df
                else:
                    self.df_full = self.df_full.append(df)

            except BaseException:
                pass

        # Write DataFrame into Meteostat database
        self.persist(self.df_full, daily_national)


# Run task
run(Task)
