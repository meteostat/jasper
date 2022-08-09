"""
Met.no hourly model import routine

Get hourly model forecasts for weather stations based on geo location

The code is licensed under the MIT license.
"""

from time import sleep
from typing import Union
from urllib import request, error
import json
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.convert import percentage_to_okta
from jasper.helpers import get_stations, read_file
from jasper.schema import hourly_model


# General configuration
STATIONS_PER_CYCLE = 42
SLEEP_TIME = 0.2

# Create Jasper instance
jsp = Jasper("import.metno.hourly.model")


def get_condicode(code: str) -> Union[int, None]:
    """
    Map Met.no symbol codes to Meteostat condicodes

    https://api.met.no/weatherapi/weathericon/2.0/documentation
    """
    condicodes = {
        "clearsky": 1,
        "cloudy": 3,
        "fair": 2,
        "fog": 5,
        "heavyrain": 9,
        "heavyrainandthunder": 26,
        "heavyrainshowers": 18,
        "heavyrainshowersandthunder": 26,
        "heavysleet": 13,
        "heavysleetandthunder": 26,
        "heavysleetshowers": 20,
        "heavysleetshowersandthunder": 26,
        "heavysnow": 16,
        "heavysnowandthunder": 26,
        "heavysnowshowers": 22,
        "heavysnowshowersandthunder": 26,
        "lightrain": 7,
        "lightrainandthunder": 25,
        "lightrainshowers": 17,
        "lightrainshowersandthunder": 25,
        "lightsleet": 12,
        "lightsleetandthunder": 25,
        "lightsleetshowers": 19,
        "lightsnow": 14,
        "lightsnowandthunder": 25,
        "lightsnowshowers": 21,
        "lightssleetshowersandthunder": 25,
        "lightssnowshowersandthunder": 25,
        "partlycloudy": 3,
        "rain": 8,
        "rainandthunder": 25,
        "rainshowers": 17,
        "rainshowersandthunder": 25,
        "sleet": 12,
        "sleetandthunder": 25,
        "sleetshowers": 19,
        "sleetshowersandthunder": 25,
        "snow": 15,
        "snowandthunder": 25,
        "snowshowers": 21,
        "snowshowersandthunder": 25,
    }

    return condicodes.get(str(code).split("_")[0], None)


# Get weather stations
stations = get_stations(
    jsp,
    read_file("model_stations.sql"),
    STATIONS_PER_CYCLE,
)

# DataFrame which holds all data
df_full = None

# Import data for each weather station
if len(stations) > 0:
    for station in stations:
        try:
            # Create request for JSON file
            url = (
                "https://api.met.no/weatherapi/locationforecast/2.0/complete.json?"
                + f"altitude={station[3]}&lat={station[1]}&lon={station[2]}"
            )
            req = request.Request(
                url, headers={"User-Agent": "meteostat.net info@meteostat.net"}
            )

            # Get JSON data
            with request.urlopen(req) as raw:
                data = json.loads(raw.read().decode())

            # pylint: disable=line-too-long
            # To be resolved in 1.0.0
            def map_data(record):
                """
                Map JSON data
                """
                if station[4]:
                    return {
                        "time": record["time"],
                        "prcp": record["data"]["next_1_hours"]["details"][
                            "precipitation_amount"
                        ]
                        if "next_1_hours" in record["data"]
                        and "precipitation_amount"
                        in record["data"]["next_1_hours"]["details"]
                        else None,
                    }
                else:
                    return {
                        "time": record["time"],
                        "temp": record["data"]["instant"]["details"]["air_temperature"]
                        if "air_temperature" in record["data"]["instant"]["details"]
                        else None,
                        "cldc": percentage_to_okta(
                            record["data"]["instant"]["details"]["cloud_area_fraction"]
                        )
                        if "cloud_area_fraction" in record["data"]["instant"]["details"]
                        else None,
                        "rhum": record["data"]["instant"]["details"][
                            "relative_humidity"
                        ]
                        if "relative_humidity" in record["data"]["instant"]["details"]
                        else None,
                        "prcp": record["data"]["next_1_hours"]["details"][
                            "precipitation_amount"
                        ]
                        if "next_1_hours" in record["data"]
                        and "precipitation_amount"
                        in record["data"]["next_1_hours"]["details"]
                        else None,
                        "wspd": record["data"]["instant"]["details"]["wind_speed"] * 3.6
                        if "wind_speed" in record["data"]["instant"]["details"]
                        else None,
                        "wpgt": record["data"]["instant"]["details"][
                            "wind_speed_of_gust"
                        ]
                        * 3.6
                        if "wind_speed_of_gust" in record["data"]["instant"]["details"]
                        else None,
                        "wdir": int(
                            round(
                                record["data"]["instant"]["details"][
                                    "wind_from_direction"
                                ]
                            )
                        )
                        if "wind_from_direction" in record["data"]["instant"]["details"]
                        else None,
                        "pres": record["data"]["instant"]["details"][
                            "air_pressure_at_sea_level"
                        ]
                        if "air_pressure_at_sea_level"
                        in record["data"]["instant"]["details"]
                        else None,
                        "coco": get_condicode(
                            record["data"]["next_1_hours"]["summary"]["symbol_code"]
                        )
                        if "next_1_hours" in record["data"]
                        and "symbol_code" in record["data"]["next_1_hours"]["summary"]
                        else None,
                    }

            # Create DataFrame
            df = pd.DataFrame(map(map_data, data["properties"]["timeseries"]))

            # Set index
            df["station"] = station[0]
            df = df.set_index(["station", "time"])

            # Shift prcp and coco columns by 1 (as they refer to the next hour)
            df["prcp"] = df["prcp"].shift(1)
            if not station[4]:
                df["coco"] = df["coco"].shift(1)

            # Append data to full DataFrame
            if df_full is None:
                df_full = df
            else:
                df_full = df_full.append(df)

        except error.HTTPError:
            pass

        # Sleep
        sleep(SLEEP_TIME)

    # Write DataFrame into Meteostat database
    persist(jsp, df_full, hourly_model)

# Close Jasper instance
jsp.close()
