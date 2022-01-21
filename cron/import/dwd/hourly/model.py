"""
DWD MOSMIX import routine

Get hourly MOSMIX model data for selected weather stations.

The code is licensed under the MIT license.
"""

import sys
import os
import re
from typing import Union
from datetime import datetime
from urllib import request
from zipfile import ZipFile
from lxml import etree
import pandas as pd
from meteor import Meteor, run
from meteor.convert import kelvin_to_celsius, ms_to_kmh, temp_dwpt_to_rhum
from meteor.schema import hourly_model


class Task(Meteor):
    """
    Import hourly (global) model data from DWD
    """

    name = 'import.dwd.hourly.model'  # Task name
    # dev_mode = True  # Running in dev mode?

    STATIONS_PER_CYCLE = 6  # Number of stations per execution
    # Path of MOSMIX stations file
    MOSMIX_PATH = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '../../../..',
            'resources'
        )
    ) + '/mosmix.csv'
    # Number of stations to skip
    skip = 0
    # DataFrame of weather stations
    stations: Union[pd.DataFrame, None] = None
    # DataFrame which holds all data
    df_full: Union[pd.DataFrame, None] = None

    @staticmethod
    def get_condicode(code: str) -> Union[int, None]:
        """
        Map DWD codes to Meteostat condicodes
        """
        condicodes = {
            '0': 1,
            '1': 2,
            '2': 3,
            '3': 4,
            '45': 5,
            '49': 5,
            '61': 7,
            '63': 8,
            '65': 9,
            '51': 7,
            '53': 8,
            '55': 9,
            '68': 12,
            '69': 13,
            '71': 14,
            '73': 15,
            '75': 16,
            '80': 17,
            '81': 18,
            '82': 18,
            '83': 19,
            '84': 20,
            '85': 21,
            '86': 22,
            '66': 10,
            '67': 11,
            '56': 10,
            '57': 11,
            '95': 25
        }

        return condicodes.get(str(code), None)

    def _update_counter(self) -> None:
        """
        Update counter value
        """
        if self.stations is None or len(
                self.stations.index) < self.STATIONS_PER_CYCLE:
            self.set_var('station_counter', 0)
            sys.exit()
        else:
            self.set_var(
                'station_counter',
                self.skip +
                self.STATIONS_PER_CYCLE)

    # pylint: disable=too-many-branches
    # Refactor in a future version
    def main(self) -> None:
        """
        Main script & entry point
        """
        self.skip = self.get_var('station_counter', 0, int)

        # Get MOSMIX stations
        try:
            self.stations = pd.read_csv(
                self.MOSMIX_PATH,
                dtype='str',
                skiprows=self.skip,
                nrows=self.STATIONS_PER_CYCLE,
                names=[
                    'id',
                    'mosmix'
                ]
            )
        except pd.errors.EmptyDataError:
            pass

        # Update counter
        self._update_counter()

        # Import data for each weather station
        # pylint: disable=no-member
        for station in self.stations.to_dict(orient='records'):
            try:
                # Load KMZ data from DWD server
                url = (
                    'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/' +
                    f"single_stations/{station['mosmix']}/kml/" +
                    f"MOSMIX_L_LATEST_{station['mosmix']}.kmz"
                )
                filename = os.path.dirname(
                    __file__) + os.sep + station['id'] + '.kmz'
                request.urlretrieve(url, filename)

                # KMZ -> KML
                with ZipFile(filename, 'r') as kmz:
                    with kmz.open(ZipFile.infolist(kmz)[0].filename, 'r') as raw:
                        kml = raw.read()

                # Remove KMZ file
                os.remove(filename)

                # Parse KML
                tree = etree.fromstring(kml)

                # Skip stale forecasts
                issue_time = datetime.strptime(
                    tree.xpath(
                        '//kml:kml/kml:Document/kml:ExtendedData/' +
                        'dwd:ProductDefinition/dwd:IssueTime',
                        namespaces=tree.nsmap
                    )[0].text,
                    '%Y-%m-%dT%H:%M:%S.%fZ'
                )
                if (datetime.now() - issue_time).total_seconds() > 25200:
                    continue

                # Collect all time steps
                timesteps = []
                for step in tree.xpath(
                    '//kml:kml/kml:Document/kml:ExtendedData/dwd:ProductDefinition/' +
                    'dwd:ForecastTimeSteps/dwd:TimeStep',
                    namespaces=tree.nsmap
                ):
                    timesteps.append(step.text)

                # COLLECT WEATHER DATA
                # Each parameter is processed individually
                data = {
                    'time': timesteps,
                    'pres': [],
                    'temp': [],
                    'dwpt': [],
                    'wdir': [],
                    'wspd': [],
                    'wpgt': [],
                    'coco': [],
                    'prcp': []
                }
                placemark = tree.xpath(
                    '//kml:kml/kml:Document/kml:Placemark',
                    namespaces=tree.nsmap
                )[0]

                # Pressure
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="PPPP"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['pres'].append(
                        float(value) / 100 if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Air temperature
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="TTT"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['temp'].append(
                        kelvin_to_celsius(float(value)) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Dew point
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="Td"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['dwpt'].append(
                        kelvin_to_celsius(float(value)) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Wind direction
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="DD"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['wdir'].append(
                        int(float(value)) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Wind speed
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="FF"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['wspd'].append(
                        ms_to_kmh(float(value)) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Peak wind gust
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="FX1"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['wpgt'].append(
                        ms_to_kmh(float(value)) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Weather condition
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="ww"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['coco'].append(
                        Task.get_condicode(int(float(value))) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Precipitation
                for value in re.sub(
                    r'/\s+/',
                    ' ',
                    placemark.xpath(
                        'kml:ExtendedData/dwd:Forecast[@dwd:elementName="RR1c"]/dwd:value',
                        namespaces=tree.nsmap
                    )[0].text
                ).strip().split():
                    data['prcp'].append(
                        float(value) if value.lstrip('-').replace(
                            '.',
                            '',
                            1
                        ).isdigit() else None
                    )

                # Convert data dict to DataFrame
                df = pd.DataFrame.from_dict(data)

                # Convert time strings to datetime
                df['time'] = pd.to_datetime(df['time'])

                # Calculate humidity data
                # pylint: disable=unnecessary-lambda
                df['rhum'] = df.apply(
                    lambda row: temp_dwpt_to_rhum(row), axis=1)

                # Drop dew point column
                df = df.drop('dwpt', axis=1)

                # Add station column
                df['station'] = station['id']

                # Set index
                df = df.set_index(['station', 'time'])

                # Round decimals
                df = df.round(1)

                # Remove tz awareness
                df = df.tz_convert(None, level='time')

                # Append data to full DataFrame
                if self.df_full is None:
                    self.df_full = df
                else:
                    self.df_full = self.df_full.append(df)

            except BaseException:
                pass

        # Write DataFrame into Meteostat database
        self.persist(self.df_full, hourly_model)


# Run task
run(Task)
