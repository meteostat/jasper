"""
Synoptic import routine

Get hourly synop data from Synoptic's Mesonet API.

The code is licensed under the MIT license.
"""

import sys
import os
from typing import Union
from datetime import datetime
import pandas as pd
from meteor import Meteor, run
from meteor.schema import hourly_synop


class Task(Meteor):
    """
    Import hourly (global) SYNOP data from Synoptic
    """

    name = 'import.synoptic.hourly.synop'  # Task name
    dev_mode = True  # Running in dev mode?

    # How many stations per cycle?
    STATIONS_PER_CYCLE = 100
    # Which parameters should be included?
    PARAMETERS = {
        'air_temp': 'temp',
        'relative_humidity': 'rhum',
        'wind_speed': 'wspd',
        'wind_direction': 'wdir',
        'wind_gust': 'wpgt',
        'sea_level_pressure': 'pres',
        'snow_depth': 'snow',
        'solar_radiation': 'srad',
        'precip_accum_one_hour': 'prcp',
        'weather_cond_code': 'coco'
    }

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get some stations
        stations = self.get_stations("""
            SELECT
                `id`,
                `synoptic`
            FROM
                `stations`
            WHERE
                `synoptic` IS NOT NULL
        """, self.STATIONS_PER_CYCLE)

        # Get STID string
        stid = ','.join(map(lambda s: s['synoptic'], stations))

# Run task
run(Task)
