"""
NOAA ISD Lite import routine

Get hourly weather data for weather stations worldwide.

The code is licensed under the MIT license.
"""

import os
import sys
from typing import Union
from datetime import datetime
from numpy import isnan
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.convert import ms_to_kmh, temp_dwpt_to_rhum
from jasper.schema import hourly_global


# General configuratiob
NOAA_ISD_LITE_ENDPOINT = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/'
MODE = sys.argv[1]
STATIONS_PER_CYCLE = 1 if MODE == "recent" else 4
USAF_WBAN_PATH = (
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..", "resources"))
    + "/usaf_wban.csv"
)
CURRENT_YEAR = datetime.now().year
# Column ranges
COLSPECS = [
    (0, 4),
    (5, 7),
    (8, 10),
    (11, 13),
    (13, 19),
    (19, 25),
    (25, 31),
    (31, 37),
    (37, 43),
    (43, 49),
    (49, 55),
]
# Column names
NAMES = ["time", "temp", "dwpt", "pres", "wdir", "wspd", "cldc", "prcp"]


def map_sky_code(code: Union[int, str]) -> Union[int, None]:
    """
    Only accept okta
    """
    return int(code) if not isnan(code) and int(code) >= 0 and int(code) <= 8 else None


# Create Jasper instance
jsp = Jasper(f"import.noaa.hourly.global.{MODE}")

# Weather stations
stations: Union[pd.DataFrame, None] = None

# Get counter value
skip = jsp.get_var("station_counter", 0, int)

# Get year
if MODE == "historical":
    year = jsp.get_var("year", 1901, int)

# Get ISD Lite stations
try:
    stations = pd.read_csv(
        USAF_WBAN_PATH,
        dtype="str",
        skiprows=skip,
        nrows=STATIONS_PER_CYCLE,
        names=["id", "usaf", "wban"],
    )
except pd.errors.EmptyDataError:
    pass

# Update counter
if stations is None or len(stations.index) < STATIONS_PER_CYCLE:
    # Reset counter
    jsp.set_var("station_counter", 0)
    # Reset year
    if MODE == "historical":
        if year >= jsp.CURRENT_YEAR - 2:
            jsp.set_var("year", 1901)
        else:
            jsp.set_var("year", year + 1)
    sys.exit()
else:
    jsp.set_var("station_counter", skip + STATIONS_PER_CYCLE)

# Get list of years
if MODE == "recent":
    years = range(CURRENT_YEAR - 1, CURRENT_YEAR + 1)
else:
    years = range(year, year + 1)

for station in stations.to_dict(orient="records"):
    for year in years:
        try:

            filename = station["usaf"] + "-" + station["wban"] + "-" + str(year) + ".gz"

            df = pd.read_fwf(
                f'{NOAA_ISD_LITE_ENDPOINT}/{year}/{filename}',
                parse_dates={"time": [0, 1, 2, 3]},
                na_values=["-9999", -9999],
                header=None,
                colspecs=COLSPECS,
                compression='gzip'
            )

            # Rename columns
            df.columns = NAMES

            # Adapt columns
            df["temp"] = df["temp"].div(10)
            df["dwpt"] = df["dwpt"].div(10)
            df["pres"] = df["pres"].div(10)
            df["wspd"] = df["wspd"].div(10).apply(ms_to_kmh)
            df["cldc"] = df["cldc"].apply(map_sky_code)
            df["prcp"] = df["prcp"].div(10)

            # Calculate humidity data
            # pylint: disable=unnecessary-lambda
            df["rhum"] = df.apply(lambda row: temp_dwpt_to_rhum(row), axis=1)

            # Drop dew point column
            # pylint: disable=no-member
            df = df.drop("dwpt", axis=1)

            # Add station column
            df["station"] = station["id"]

            # Set index
            df = df.set_index(["station", "time"])

            # Round decimals
            df = df.round(1)

            # Write data into Meteostat database
            persist(jsp, df, hourly_global)

        except BaseException:
            pass

# Close Jasper instance
jsp.close()
