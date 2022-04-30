"""
Get observations from api.weather.gov

The code is licensed under the MIT license.
"""

from urllib import request, error
import json
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.helpers import get_stations, read_file
from jasper.schema import hourly_national


# General configuration
STATIONS_PER_CYCLE = 36

# Create Jasper instance
jsp = Jasper("import.noaa.hourly.national_metar")

# Get weather stations
stations = get_stations(
    jsp, read_file("national_metar_stations.sql"), STATIONS_PER_CYCLE
)

# DataFrame which holds all data
df_full = None

# Import data for each weather station
if len(stations) > 0:
    for station in stations:
        try:
            # Create request for JSON file
            url = f"https://api.weather.gov/stations/{station['icao']}/observations/latest"
            req = request.Request(
                url, headers={"User-Agent": "meteostat.net info@meteostat.net"}
            )

            # Get JSON data
            with request.urlopen(req) as raw:
                data = json.loads(raw.read().decode())["properties"]

            record = {
                "time": data["timestamp"],
                "temp": data["temperature"]["value"],
                "rhum": data["relativeHumidity"]["value"],
                "wdir": data["windDirection"]["value"],
                "wspd": data["windSpeed"]["value"],
                "pres": data["seaLevelPressure"]["value"] / 100
                if data["seaLevelPressure"]["value"]
                else None,
            }

            # Create DataFrame
            df = pd.DataFrame([record])

            # Set index
            df["station"] = station[0]
            df["time"] = pd.to_datetime(df["time"])
            df = df.set_index(["station", "time"])

            # Append data to full DataFrame
            if df_full is None:
                df_full = df
            else:
                df_full = df_full.append(df)

        except error.HTTPError:
            pass

    # Write DataFrame into Meteostat database
    persist(jsp, df_full.round(1), hourly_national)

# Close Jasper instance
jsp.close()
