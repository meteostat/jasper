"""
DWD MOSMIX import routine
Get hourly MOSMIX model data for selected weather stations.
The code is licensed under the MIT license.
"""

import os
import re
from typing import Union
from datetime import datetime
from urllib import request
from zipfile import ZipFile
from lxml import etree
import pandas as pd
from jasper import Jasper
from jasper.convert import (
    jcm2_to_wm2,
    kelvin_to_celsius,
    ms_to_kmh,
    percentage_to_okta,
    temp_dwpt_to_rhum,
)
from jasper.helpers import get_stations, read_file
from jasper.schema import hourly_model
from jasper.actions import persist


# General configuration
STATIONS_PER_CYCLE = 6  # Number of stations per execution

# Variables
df_full: Union[pd.DataFrame, None] = None  # DataFrame which holds all data

# Create Jasper instance
jsp = Jasper("import.dwd.hourly.model")


def get_condicode(code: str) -> Union[int, None]:
    """
    Map DWD codes to Meteostat condicodes
    """
    condicodes = {
        "0": 1,
        "1": 2,
        "2": 3,
        "3": 4,
        "45": 5,
        "49": 5,
        "61": 7,
        "63": 8,
        "65": 9,
        "51": 7,
        "53": 8,
        "55": 9,
        "68": 12,
        "69": 13,
        "71": 14,
        "73": 15,
        "75": 16,
        "80": 17,
        "81": 18,
        "82": 18,
        "83": 19,
        "84": 20,
        "85": 21,
        "86": 22,
        "66": 10,
        "67": 11,
        "56": 10,
        "57": 11,
        "95": 25,
    }

    return condicodes.get(str(code), None)


# Get MOSMIX weather stations
stations = get_stations(
    jsp,
    read_file("model_stations.sql"),
    STATIONS_PER_CYCLE,
)

# Import data for each weather station
for station in stations:
    try:
        # Load KMZ data from DWD server
        url = (
            "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/"
            + f"single_stations/{station['mosmix']}/kml/"
            + f"MOSMIX_L_LATEST_{station['mosmix']}.kmz"
        )
        filename = os.path.dirname(__file__) + os.sep + station["id"] + ".kmz"
        request.urlretrieve(url, filename)

        # KMZ -> KML
        with ZipFile(filename, "r") as kmz:
            with kmz.open(ZipFile.infolist(kmz)[0].filename, "r") as raw:
                kml = raw.read()

        # Remove KMZ file
        os.remove(filename)

        # Parse KML
        tree = etree.fromstring(kml)

        # Skip stale forecasts
        issue_time = datetime.strptime(
            tree.xpath(
                "//kml:kml/kml:Document/kml:ExtendedData/"
                + "dwd:ProductDefinition/dwd:IssueTime",
                namespaces=tree.nsmap,
            )[0].text,
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        if (datetime.now() - issue_time).total_seconds() > 25200:
            continue

        # Collect all time steps
        timesteps = []
        for step in tree.xpath(
            "//kml:kml/kml:Document/kml:ExtendedData/dwd:ProductDefinition/"
            + "dwd:ForecastTimeSteps/dwd:TimeStep",
            namespaces=tree.nsmap,
        ):
            timesteps.append(step.text)

        # COLLECT WEATHER DATA
        # Each parameter is processed individually
        data = {
            "time": timesteps,
            "temp": [],
            "dwpt": [],
            "prcp": [],
            "wdir": [],
            "wspd": [],
            "wpgt": [],
            "tsun": [],
            "srad": [],
            "pres": [],
            "cldc": [],
            "vsby": [],
            "coco": [],
        }
        placemark = tree.xpath(
            "//kml:kml/kml:Document/kml:Placemark", namespaces=tree.nsmap
        )[0]

        # Pressure
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="PPPP"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["pres"].append(
                float(value) / 100
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Air temperature
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="TTT"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["temp"].append(
                kelvin_to_celsius(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Dew point
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="Td"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["dwpt"].append(
                kelvin_to_celsius(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Wind direction
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="DD"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["wdir"].append(
                int(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Wind speed
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="FF"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["wspd"].append(
                ms_to_kmh(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Peak wind gust
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="FX1"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["wpgt"].append(
                ms_to_kmh(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Weather condition
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="ww"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["coco"].append(
                get_condicode(int(float(value)))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Precipitation
        # !!!!!!!!
        # PRCP DATA SEEMS INCORRECT
        # NEEDS INVESTIGATION
        # !!!!!!!!
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="RR1c"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["prcp"].append(
                # float(value)
                # if value.lstrip("-").replace(".", "", 1).isdigit()
                # else None
                None
            )

        # Sunshine Duration
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="SunD1"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["tsun"].append(
                float(value) / 60
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Solar Radiation
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="Rad1h"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["srad"].append(
                float(value) / 10
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Cloud Cover
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="N"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["cldc"].append(
                percentage_to_okta(float(value))
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Visibility
        for value in (
            re.sub(
                r"/\s+/",
                " ",
                placemark.xpath(
                    'kml:ExtendedData/dwd:Forecast[@dwd:elementName="VV"]/dwd:value',
                    namespaces=tree.nsmap,
                )[0].text,
            )
            .strip()
            .split()
        ):
            data["vsby"].append(
                float(value)
                if value.lstrip("-").replace(".", "", 1).isdigit()
                else None
            )

        # Convert data dict to DataFrame
        df = pd.DataFrame.from_dict(data)

        # Convert time strings to datetime
        df["time"] = pd.to_datetime(df["time"])

        # Convert SRAD to W/M^2
        df["srad"] = df["srad"].apply(jcm2_to_wm2)

        # Calculate humidity data
        # pylint: disable=unnecessary-lambda
        df["rhum"] = df.apply(lambda row: temp_dwpt_to_rhum(row), axis=1)

        # Drop dew point column
        df = df.drop("dwpt", axis=1)

        # Add station column
        df["station"] = station["id"]

        # Set index
        df = df.set_index(["station", "time"])

        # Round decimals
        df = df.round(1)

        # Remove tz awareness
        df = df.tz_convert(None, level="time")

        # Append data to full DataFrame
        if df_full is None:
            df_full = df
        else:
            df_full = df_full.append(df)

    except BaseException:
        pass

# Write DataFrame into Meteostat database
persist(jsp, df_full, hourly_model)

# Close Jasper instance
jsp.close()
