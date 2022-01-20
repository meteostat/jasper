"""
NOAA hourly METAR data import routine

Get hourly METAR data from NOAA.

The code is licensed under the MIT license.
"""

from typing import Union
from datetime import datetime, timedelta
from urllib import request
import pandas as pd
from metar import Metar
from meteor import Meteor, run
from meteor.convert import temp_dwpt_to_rhum
from meteor.schema import hourly_metar


class Task(Meteor):
    """
    Import (global) METAR data
    """

    name = 'import.noaa.hourly.metar'  # Task name
    # dev_mode = True  # Running in dev mode?

    @staticmethod
    def get_condicode(weather: list) -> Union[int, None]:
        """
        Map METAR codes to Meteostat condicodes
        """
        try:
            code = weather[0][3]

            condicodes = {
                'RA': 8,
                'SHRA': 17,
                'DZ': 7,
                'DZRA': 7,
                'FZRA': 10,
                'FZDZ': 10,
                'RASN': 12,
                'SN': 15,
                'SHSN': 21,
                'SG': 12,
                'IC': 12,
                'PL': 24,
                'GR': 24,
                'GS': 24,
                'FG': 5,
                'BR': 5,
                'MIFG': 5,
                'BCFG': 5,
                'HZ': 5,
                'TS': 25,
                'TSRA': 25,
                'PO': 27,
                'SQ': 27,
                'FC': 27,
                'SS': 27,
                'DS': 27
            }

            return condicodes.get(str(code), None)

        except BaseException:
            return None

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get ICAO stations
        stations = pd.read_sql(
            'SELECT `id`, `icao` FROM `stations` WHERE `icao` IS NOT NULL',
            self.db
        )

        stations = stations.set_index('icao')

        # Get cycle
        cycle = (datetime.now() - timedelta(hours=2)).strftime('%H')

        # Create request for JSON file
        url = f'https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{cycle}Z.TXT'
        req = request.Request(url)

        # Get METAR strings
        with request.urlopen(req) as raw:
            file = raw.read().decode(errors='ignore').splitlines()

        data = []

        for line in file:
            # Skip non-METAR lines
            if not line[:2].isalpha():
                continue

            try:
                # Parse METAR string
                obs = Metar.Metar(line)

                if obs.station_id in stations.index:

                    # pylint: disable=line-too-long
                    data.append({
                        'station': stations.loc[obs.station_id][0] if obs.station_id is not None else None,
                        'time': obs.time,
                        'temp': obs.temp.value('C') if obs.temp is not None else None,
                        'dwpt': obs.dewpt.value('C') if obs.dewpt is not None else None,
                        'wdir': obs.wind_dir.value() if (
                            obs.wind_dir is not None and
                            obs.wind_dir.value() > 0
                        ) else None,
                        'wspd': obs.wind_speed.value('KMH') if (
                            obs.wind_speed is not None and
                            obs.wind_speed.value('KMH') > 0
                        ) else None,
                        'pres': obs.press.value('HPA') if obs.press is not None else None,
                        'coco': Task.get_condicode(obs.weather) if obs.weather is not None else None
                    })

            except BaseException:
                pass

        # List -> DataFrame
        df = pd.DataFrame.from_records(data)

        # Calculate humidity data
        # pylint: disable=unnecessary-lambda
        df['rhum'] = df.apply(
            lambda row: temp_dwpt_to_rhum(row),
            axis=1
        )

        # Drop dew point column
        df = df.drop('dwpt', axis=1)

        # Set index
        df = df.set_index(['station', 'time'])

        # Round decimals
        df = df.round(1)

        # Write DataFrame into Meteostat database
        self.persist(df, hourly_metar)


run(Task)
