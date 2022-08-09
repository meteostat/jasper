"""
DWD daily solar radiation data

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
from jasper.convert import jcm2_to_wm2
from jasper.schema import daily_national
from jasper.actions import persist


# General configuration
DWD_FTP_SERVER = "opendata.dwd.de"  # DWD open data server
STATIONS_PER_CYCLE = int(sys.argv[1])  # Number of stations per execution
USECOLS = [1, 5]  # CSV cols which should be read
PARSE_DATES = {"time": [0]}  # Which columns should be parsed as dates?
NAMES = {"FG_STRAHL": "srad"}

# Variables
counter = 0  # The task's counter
skip = 3  # How many lines to skip
ftp: FTP = None  # DWD FTP connection
files = []  # List of remote files
df_full: pd.DataFrame = None  # DataFrame which holds all data

# Create Jasper instance
jsp = Jasper(f"import.dwd.daily.national_srad")


def dateparser(value):
    """
    Custom Pandas date parser
    """
    return datetime.strptime(value, "%Y%m%d")


# Connect to DWD FTP server
ftp = FTP(DWD_FTP_SERVER)
ftp.login()
ftp.cwd("/climate_environment/CDC/observations_germany/climate/daily/solar/")

# Get counter value
counter = jsp.get_var("station_counter", 0, int)
skip = skip if counter == 0 else skip + counter

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
        national_id = str(remote_file[-13:-8])
        station_df = pd.read_sql(
            f"SELECT `id` FROM `stations` WHERE `national_id` LIKE '{national_id}'",
            jsp.db(),
        )
        station = station_df.iloc[0][0]

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
            sep=r"\s*;\s*",
            date_parser=dateparser,
            na_values=["-999", -999],
            usecols=USECOLS,
            parse_dates=PARSE_DATES,
        )

        # Rename columns
        df = df.rename(columns=lambda x: x.strip())
        df = df.rename(columns=NAMES)

        # Convert to W/M^2
        df["srad"] = df["srad"].divide(24).apply(jcm2_to_wm2)

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
