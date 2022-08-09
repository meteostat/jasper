"""
DWD POI import routine

Get hourly synop data for selected weather stations in Europe.

The code is licensed under the MIT license.
"""

import sys
import os
from typing import Union
from datetime import datetime
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.convert import percentage_to_okta
from jasper.schema import hourly_synop


# General configuration
STATIONS_PER_CYCLE = 10  # Number of weather stations per execution
# Path of POI stations file
POI_PATH = (
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..", "resources"))
    + "/poi.csv"
)
# Column which should be imported
USECOLS = [0, 1, 2, 9, 11, 14, 21, 22, 23, 33, 35, 36, 37, 40, 41]
PARSE_DATES = {"time": [0, 1]}  # Columns which should be parsed as dates
# Column names
NAMES = {
    "Wolkenbedeckung": "cldc",
    "Temperatur (2m)": "temp",
    "Globalstrahlung (letzte Stunde)": "srad",
    "Sichtweite": "vsby",
    "Windgeschwindigkeit": "wspd",
    "Windboen (letzte Stunde)": "wpgt",
    "Niederschlag (letzte Stunde)": "prcp",
    "Relative Feuchte": "rhum",
    "Windrichtung": "wdir",
    "Druck (auf Meereshoehe)": "pres",
    "Sonnenscheindauer (letzte Stunde)": "tsun",
    "aktuelles Wetter": "coco",
    "Schneehoehe": "snow",
}

# Variables
stations: Union[pd.DataFrame, None] = None  # DataFrame of weather stations

# Create Jasper instance
jsp = Jasper("import.dwd.hourly.synop")


def dateparser(date, hour):
    """
    Custom Pandas date parser
    """
    return datetime.strptime(f"{date} {hour}", "%d.%m.%y %H:%M")


def get_condicode(code: str) -> Union[int, None]:
    """
    Map DWD codes to Meteostat condicodes

    Check docs/dwd_poi_codes.pdf for more information
    """
    condicodes = {
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7": 7,
        "8": 8,
        "9": 9,
        "10": 10,
        "11": 11,
        "12": 12,
        "13": 13,
        "14": 14,
        "15": 15,
        "16": 16,
        "17": 24,
        "18": 17,
        "19": 18,
        "20": 19,
        "21": 20,
        "22": 21,
        "23": 22,
        "24": 19,
        "25": 20,
        "26": 23,
        "27": 25,
        "28": 26,
        "29": 25,
        "30": 26,
        "31": 27,
    }

    return condicodes.get(str(code), None)


# Get counter value
skip = jsp.get_var("station_counter", 0, int)

# Get POI stations
try:
    stations = pd.read_csv(
        POI_PATH,
        dtype="str",
        skiprows=skip,
        nrows=STATIONS_PER_CYCLE,
        names=["id"],
    )
except pd.errors.EmptyDataError:
    pass

# Update counter
if stations is None or len(stations.index) < STATIONS_PER_CYCLE:
    jsp.set_var("station_counter", 0)
    sys.exit()
else:
    jsp.set_var("station_counter", skip + STATIONS_PER_CYCLE)

# DataFrame which holds all data
df_full = None

# Import data for each weather station
# pylint: disable=no-member
for station in stations.to_dict(orient="records"):
    try:
        # Read CSV data from DWD server
        url = f"https://opendata.dwd.de/weather/weather_reports/poi/{station['id']}-BEOB.csv"
        df = pd.read_csv(
            url,
            sep=";",
            skiprows=2,
            na_values="---",
            usecols=USECOLS,
            decimal=",",
            parse_dates=PARSE_DATES,
            date_parser=dateparser,
        )

        # Rename columns
        df = df.rename(columns=NAMES)

        # Snow cm -> mm
        df["snow"] = df["snow"].multiply(10)
        df["vsby"] = df["vsby"].multiply(1000)

        # Change coco
        df["coco"] = df["coco"].apply(get_condicode)
        df["cldc"] = df["cldc"].apply(percentage_to_okta)

        # Add station column
        df["station"] = station["id"]

        # Set index
        df = df.set_index(["station", "time"])

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except BaseException:
        pass

# Write DataFrame into Meteostat database
persist(jsp, df_full, hourly_synop)

# Close Jasper instance
jsp.close()
