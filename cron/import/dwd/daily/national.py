"""
DWD national daily data import task

Get daily data for weather stations in Germany.

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
import sys
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime
import hashlib
import pandas as pd
from jasper import Jasper
from jasper.convert import pres_to_msl, ms_to_kmh
from jasper.schema import daily_national
from jasper.actions import persist


# General configuration
MODE = sys.argv[1]  # 'recent' or 'historical'
DWD_FTP_SERVER = "opendata.dwd.de"  # DWD open data server
STATIONS_PER_CYCLE = int(sys.argv[2])  # Number of stations per execution
USECOLS = [1, 3, 4, 6, 8, 9, 12, 13, 14, 15, 16]  # CSV cols which should be read
PARSE_DATES = {"time": [0]}  # Which columns should be parsed as dates?
NAMES = {
    "FX": "wpgt",
    "FM": "wspd",
    "RSK": "prcp",
    "SDK": "tsun",
    "SHK_TAG": "snow",
    "PM": "pres",
    "TMK": "tavg",
    "UPM": "rhum",
    "TXK": "tmax",
    "TNK": "tmin",
}

# Variables
counter = 0  # The task's counter
skip = 3  # How many lines to skip
ftp: FTP = None  # DWD FTP connection
files = []  # List of remote files
df_full: pd.DataFrame = None  # DataFrame which holds all data

# Create Jasper instance
jsp = Jasper(f"import.dwd.daily.national.{MODE}")


def dateparser(value):
    """
    Custom Pandas date parser
    """
    return datetime.strptime(value, "%Y%m%d")


# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd("/climate_environment/CDC/observations_germany/climate/daily/kl/" + MODE)

# Get counter value
counter = jsp.get_var("station_counter", 0, int)
skip = 3 if counter == 0 else 3 + counter

# Get all files in directory
try:
    endpos = STATIONS_PER_CYCLE + skip
    files = ftp.nlst()[skip:endpos]
except BaseException:
    pass

# Update counter value
if files is None or len(files) < STATIONS_PER_CYCLE:
    jsp.set_var("station_counter", 0)
    sys.exit()
else:
    jsp.set_var("station_counter", counter + STATIONS_PER_CYCLE)

# Process each file
for remote_file in files:
    try:
        # Get meta info for weather station
        national_id = (
            str(remote_file[-13:-8]) if MODE == "recent" else str(remote_file[-32:-27])
        )
        station_df = pd.read_sql(
            f"SELECT `id`, `altitude` FROM `stations` WHERE `national_id` LIKE '{national_id}'",
            jsp.db(),
        )
        station = station_df.iloc[0][0]
        altitude = station_df.iloc[0][1]

        # Get remote file
        file_hash = hashlib.md5(remote_file.encode("utf-8")).hexdigest()
        local_file = os.path.dirname(__file__) + os.sep + file_hash
        # pylint: disable=consider-using-with
        ftp.retrbinary("RETR " + remote_file, open(local_file, "wb").write)

        # Unzip file
        zipped = ZipFile(local_file, "r")
        filelist = zipped.namelist()
        raw = None
        for file in filelist:
            if file[:7] == "produkt":
                raw = BytesIO(zipped.open(file, "r").read())

        # Remove ZIP file
        os.remove(local_file)

        # Convert raw data to DataFrame
        df: pd.DataFrame = pd.read_csv(
            raw,
            sep=";",
            date_parser=dateparser,
            na_values=["-999", -999],
            usecols=USECOLS,
            parse_dates=PARSE_DATES,
        )

        # Rename columns
        df = df.rename(columns=lambda x: x.strip())
        df = df.rename(columns=NAMES)

        # Convert PRES to MSL
        df["pres"] = df.apply(lambda row, alt=altitude: pres_to_msl(row, alt), axis=1)
        df["wpgt"] = df["wpgt"].apply(ms_to_kmh)
        df["wspd"] = df["wspd"].apply(ms_to_kmh)
        df["tsun"] = df["tsun"] * 60
        df["snow"] = df["snow"] * 10

        # Add weather station ID
        df["station"] = station

        # Set index
        df = df.set_index(["station", "time"])

        # Round decimals
        df = df.round(1)

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except BaseException:
        pass

# Write DataFrame into Meteostat database
persist(jsp, df_full, daily_national)

# Close Jasper instance
jsp.close()
