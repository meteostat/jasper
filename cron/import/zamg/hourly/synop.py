"""
ZAMG hourly synop import routine

Get hourly synop data for selected weather stations in Austria.

The code is licensed under the MIT license.
"""

from datetime import datetime
import pandas as pd
from meteor import Meteor, run
from meteor.schema import hourly_synop


class Task(Meteor):
    """
    Import hourly (national) SYNOP data from ZAMG
    """

    name = 'import.zamg.hourly.synop'  # Task name
    # dev_mode = True  # Running in dev mode?

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

    @staticmethod
    def dateparser(date: str, hour: str):
        """
        Custom Pandas date parser
        """
        return datetime.strptime(f'{date} {hour}', '%d-%m-%Y %H:%M')

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Read CSV data
        df: pd.DataFrame = pd.read_csv(
            'http://www.zamg.ac.at/ogd/',
            sep=';',
            parse_dates=self.parse_dates,
            date_parser=Task.dateparser,
            usecols=self.usecols,
            decimal=','
        )

        # Rename columns
        df = df.rename(columns=self.names)

        # Set index
        df = df.set_index(['station', 'time'])

        # Convert time zone to UTC
        df = df.tz_localize('Europe/Vienna', level='time')
        df = df.tz_convert(None, level='time')

        # Convert sunshine from percent to minutes
        df['tsun'] = round(60 * (df['tsun'] / 100))

        # Write DataFrame into Meteostat database
        self.persist(df, hourly_synop)


run(Task)
