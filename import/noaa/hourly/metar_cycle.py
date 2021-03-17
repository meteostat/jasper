"""
NOAA hourly METAR data import routine

Get hourly METAR data from NOAA.

The code is licensed under the MIT license.
"""

from datetime import datetime, timedelta
from urllib import request, error
import pandas as pd
from metar import Metar
from metar.Datatypes import (
    temperature,
    pressure,
    speed,
    distance,
    direction,
    precipitation,
)
from routines import Routine
from routines.convert import temp_dwpt_to_rhum
from routines.schema import hourly_metar

task = Routine('import.noaa.hourly.metar')

# Map METAR codes to Meteostat condicodes
def get_condicode(weather: list):

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

    except:

        return None

# Get ICAO stations
stations = task.read("""SELECT `id`, `icao` FROM `stations` WHERE `icao` IS NOT NULL""")
stations = stations.set_index('icao')

# Get cycle
cycle = (datetime.now() - timedelta(hours = 2)).strftime('%H')

# Create request for JSON file
url = f"https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{cycle}Z.TXT"
req = request.Request(url)

# Get METAR strings
with request.urlopen(req) as raw:
    file = raw.read().decode().splitlines()

data = []

for line in file:

    # Skip non-METAR lines
    if not line[:2].isalpha():
        continue

    try:

        # Parse METAR string
        obs = Metar.Metar(line)

        if obs.station_id in stations.index:

            data.append({
                'station': stations.loc[obs.station_id][0] if obs.station_id is not None else None,
                'time': obs.time,
                'temp': obs.temp.value('C') if obs.temp is not None else None,
                'dwpt': obs.dewpt.value('C') if obs.dewpt is not None else None,
                'wdir': obs.wind_dir.value() if obs.wind_dir is not None and obs.wind_dir.value() > 0 else None,
                'wspd': obs.wind_speed.value('KMH') if obs.wind_speed is not None and obs.wind_speed.value('KMH') > 0 else None,
                'pres': obs.press.value('HPA') if obs.press is not None else None,
                'coco': get_condicode(obs.weather) if obs.weather is not None else None
            })

    except:

        pass

# List -> DataFrame
df = pd.DataFrame.from_records(data)

# Calculate humidity data
df['rhum'] = df.apply(lambda row: temp_dwpt_to_rhum(row), axis=1)

# Drop dew point column
df = df.drop('dwpt', axis=1)

# Set index
df = df.set_index(['station', 'time'])

# Round decimals
df = df.round(1)

# Write DataFrame into Meteostat database
task.write(df, hourly_metar)
