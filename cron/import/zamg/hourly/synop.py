"""
ZAMG hourly synop import routine

Get hourly synop data for selected weather stations in Austria.

The code is licensed under the MIT license.
"""

from datetime import datetime
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.schema import hourly_synop


# General configuration
PARSE_DATES = {"time": [1, 2]}
USECOLS = [0, 3, 4, 5, 7, 8, 9, 11, 12, 13, 15]
NAMES = {
    "Station": "station",
    "T °C": "temp",
    "RF %": "rhum",
    "WR °": "wdir",
    "WG km/h": "wspd",
    "WSG km/h": "wpgt",
    "N l/m²": "prcp",
    "LDred hPa": "pres",
    "SO %": "tsun",
}

# Create Jasper instance
jsp = Jasper("import.zamg.hourly.synop")


def dateparser(date: str, hour: str):
    """
    Custom Pandas date parser
    """
    return datetime.strptime(f"{date} {hour}", "%d-%m-%Y %H:%M")


# Read CSV data
df: pd.DataFrame = pd.read_csv(
    "http://www.zamg.ac.at/ogd/",
    sep=";",
    parse_dates=PARSE_DATES,
    date_parser=dateparser,
    usecols=USECOLS,
    decimal=",",
)

# Rename columns
df = df.rename(columns=NAMES)

# Set index
df = df.set_index(["station", "time"])

# Convert time zone to UTC
df = df.tz_localize("Europe/Vienna", level="time")
df = df.tz_convert(None, level="time")

# Convert sunshine from percent to minutes
df["tsun"] = round(60 * (df["tsun"] / 100))

# Write DataFrame into Meteostat database
persist(jsp, df, hourly_synop)
