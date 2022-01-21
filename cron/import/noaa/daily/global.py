"""
NOAA GHCND import routine

Get daily weather data for weather stations worldwide.

The code is licensed under the MIT license.
"""

import sys
import os
from typing import Union
import pandas as pd
from numpy import nan
from meteor import Meteor, run, ghcnd
from meteor.convert import ms_to_kmh
from meteor.schema import daily_global


class Task(Meteor):
    """
    Import daily (global) data from NOAA
    """

    name = 'import.noaa.daily.global'  # Task name
    # dev_mode = True  # Running in dev mode?

    STATIONS_PER_CYCLE = 1
    GHCN_PATH = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '../../../..',
            'resources'
        )
    ) + '/ghcn.csv'
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
    stations: Union[pd.DataFrame, None] = None

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get counter value
        skip = self.get_var('station_counter', 0, int)

        # Get GHCN stations
        try:
            self.stations = pd.read_csv(
                self.GHCN_PATH,
                dtype='str',
                skiprows=skip,
                nrows=self.STATIONS_PER_CYCLE,
                names=[
                    'id',
                    'ghcn'
                ]
            )
        except pd.errors.EmptyDataError:
            pass

        # Update counter
        if self.stations is None or len(
                self.stations.index) < self.STATIONS_PER_CYCLE:
            self.set_var('station_counter', 0)
            sys.exit()
        else:
            self.set_var('station_counter', skip + self.STATIONS_PER_CYCLE)

        # DataFrame which holds all data
        df_full = None

        # Connect to NOAA FTP Server
        ftp = ghcnd.connect_to_ftp()

        # Import data for each weather station
        # pylint: disable=no-member
        for station in self.stations.to_dict(orient='records'):
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
                        col for col in df if col not in required_columns
                    ]
                )

                # Add missing columns
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = nan

                # Rename columns
                df = df.reset_index().rename(columns=self.names)

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
        self.persist(df_full, daily_global)

        # Quit FTP connection
        ftp.quit()


run(Task)
