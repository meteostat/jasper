"""
DWD national hourly data import routine

Get hourly data for weather stations in Germany.

The code is licensed under the MIT license.
"""

import os
from io import BytesIO
import sys
from typing import Union
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime
import hashlib
import pandas as pd
from jasper import Jasper
from jasper.convert import ms_to_kmh
from jasper.schema import hourly_national
from jasper.actions import persist


# General configuration
MODE = sys.argv[1]  # 'recent' or 'historical'
DWD_FTP_SERVER = "opendata.dwd.de"  # DWD open data server
BASE_DIR = f"precipitation/{MODE}"  # Base directory on DWD server
STATIONS_PER_CYCLE = int(sys.argv[2])  # Number of weather stations per execution
# Parameter config
PARAMETERS = [
    {
        "dir": f"precipitation/{MODE}",
        "usecols": [1, 3],
        "parse_dates": {"time": [0]},
        "names": {"R1": "prcp"},
        "convert": {},
    },
    {
        "dir": f"air_temperature/{MODE}",
        "usecols": [1, 3, 4],
        "parse_dates": {"time": [0]},
        "names": {"TT_TU": "temp", "RF_TU": "rhum"},
        "convert": {},
    },
    {
        "dir": f"wind/{MODE}",
        "usecols": [1, 3, 4],
        "parse_dates": {"time": [0]},
        "names": {"F": "wspd", "D": "wdir"},
        "convert": {"wspd": ms_to_kmh},
    },
    {
        "dir": f"pressure/{MODE}",
        "usecols": [1, 3],
        "parse_dates": {"time": [0]},
        "names": {"P": "pres"},
        "convert": {},
    },
    {
        "dir": f"sun/{MODE}",
        "usecols": [1, 3],
        "parse_dates": {"time": [0]},
        "names": {"SD_SO": "tsun"},
        "convert": {},
    },
]

# Variables
ftp: Union[FTP, None] = None  # FTP server connection
counter = 0  # The task's counter
skip = 3  # How many lines to skip
stations = []  # List of weather stations
df_full: Union[pd.DataFrame, None] = None  # DataFrame which holds all data

# Create Jasper instance
jsp = Jasper(f"import.dwd.hourly.national.{MODE}")


def dateparser(value):
    """
    Custom Pandas date parser
    """
    return datetime.strptime(value, "%Y%m%d%H")


def find_file(path: str, needle: str):
    """
    Find file in directory
    """
    match = None

    try:
        ftp.cwd("/climate_environment/CDC/observations_germany/climate/hourly/" + path)
        files = ftp.nlst()
        matching = [f for f in files if needle in f]
        match = matching[0]
    except BaseException:
        pass

    return match


# Connect to FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd(f"/climate_environment/CDC/observations_germany/climate/hourly/{BASE_DIR}")

# Get counter value
counter = jsp.get_var("station_counter", 0, int)
skip = 3 if counter == 0 else 3 + counter

# Get all files in directory
try:
    endpos = STATIONS_PER_CYCLE + skip
    stations = ftp.nlst()[skip:endpos]
except BaseException:
    pass

# Update counter
if len(stations) < STATIONS_PER_CYCLE:
    jsp.set_var("station_counter", 0)
    sys.exit()
else:
    jsp.set_var("station_counter", counter + STATIONS_PER_CYCLE)

for station_file in stations:
    try:
        # Get national weather station ID
        national_id = (
            str(station_file[-13:-8])
            if MODE == "recent"
            else str(station_file[-32:-27])
        )
        station = pd.read_sql(
            f"""
            SELECT `id` FROM `stations` WHERE `national_id` LIKE "{national_id}"
            """,
            jsp.db(),
        ).iloc[0][0]

        # DataFrame which holds data for one weather station
        df_station = None

        # Go through all parameters
        for parameter in PARAMETERS:
            try:
                remote_file = find_file(parameter["dir"], national_id)

                if remote_file is not None:
                    file_hash = hashlib.md5(remote_file.encode("utf-8")).hexdigest()
                    local_file = os.path.dirname(__file__) + os.sep + file_hash
                    with open(local_file, "wb") as f:
                        ftp.retrbinary("RETR " + remote_file, f.write)

                    # Unzip file
                    with ZipFile(local_file, "r") as zipped:
                        filelist = zipped.namelist()
                        raw = None
                        for file in filelist:
                            if file[:7] == "produkt":
                                with zipped.open(file, "r") as reader:
                                    raw = BytesIO(reader.read())

                    # Remove ZIP file
                    os.remove(local_file)

                    # Convert raw data to DataFrame
                    df: pd.DataFrame = pd.read_csv(
                        raw,
                        sep=";",
                        date_parser=dateparser,
                        na_values="-999",
                        usecols=parameter["usecols"],
                        parse_dates=parameter["parse_dates"],
                    )

                    # Rename columns
                    df = df.rename(columns=lambda x: x.strip())
                    df = df.rename(columns=parameter["names"])

                    # Convert column data
                    for col, func in parameter["convert"].items():
                        df[col] = df[col].apply(func)

                    # Add weather station ID
                    df["station"] = station

                    # Set index
                    df = df.set_index(["station", "time"])

                    # Round decimals
                    df = df.round(1)

                    # Append data to full DataFrame
                    if df_station is None:
                        df_station = df
                    else:
                        df_station = df_station.join(df)

            except BaseException:
                pass

        # Append data to full DataFrame
        if df_full is None:
            df_full = df_station
        else:
            df_full = df_full.append(df_station)

    except BaseException:
        pass

# Write DataFrame into Meteostat database
persist(jsp, df_full, hourly_national)

# Close Jasper instance
jsp.close()
