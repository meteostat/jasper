"""
Get latest weather station meta data from GitHub

The code is licensed under the MIT license.
"""

import json
import re
import urllib.request as request
import zipfile
from jasper import Jasper
from jasper.helpers import read_file


# Create Jasper instance
jsp = Jasper("import.internal.github.stations")


def write_station(data: dict) -> None:
    """
    Add a new weather station or update existing
    """
    try:
        # Break if invalid data
        if len(data["id"]) != 5 or len(data["country"]) != 2:
            return None

        # Extract alternate names
        name_alt = {key: data["name"][key] for key in data["name"] if key != "en"}

        jsp.query(
            read_file("stations_import.sql"),
            {
                "id": data["id"],
                "country": data["country"],
                "region": data["region"],
                "name": data["name"]["en"],
                "name_alt": json.dumps(name_alt, ensure_ascii=False, sort_keys=True),
                "national_id": data["identifiers"]["national"]
                if ("national" in data["identifiers"])
                else None,
                "wmo": data["identifiers"]["wmo"]
                if "wmo" in data["identifiers"]
                else None,
                "icao": data["identifiers"]["icao"]
                if "icao" in data["identifiers"]
                else None,
                "iata": data["identifiers"]["iata"]
                if "iata" in data["identifiers"]
                else None,
                "ghcn": data["identifiers"]["ghcn"]
                if "ghcn" in data["identifiers"]
                else None,
                "wban": data["identifiers"]["wban"]
                if "wban" in data["identifiers"]
                else None,
                "usaf": data["identifiers"]["usaf"]
                if "usaf" in data["identifiers"]
                else None,
                "lat": data["location"]["latitude"],
                "lon": data["location"]["longitude"],
                "elevation": data["location"]["elevation"],
                "tz": data["timezone"],
            },
        )

    except BaseException:
        pass


# Create copy of stations table
jsp.query("CREATE TABLE `stations_temp` LIKE `stations`")

try:
    # Load station repository
    handle, _ = request.urlretrieve(
        "https://github.com/meteostat/weather-stations/archive/refs/heads/master.zip"
    )
    zip_obj = zipfile.ZipFile(handle, "r")

    # Write all stations
    for index, name in enumerate(zip_obj.namelist()):
        if re.search("/stations/([A-Z0-9]{5}).json$", name):
            file = zip_obj.namelist()[index]
            raw = zip_obj.open(file)
            data = json.loads(raw.read().decode("UTF-8"))
            write_station(data)

    # Remove existing table
    jsp.query("DROP TABLE `stations`")

    # Rename temp table
    jsp.query("ALTER TABLE `stations_temp` RENAME `stations`")

except BaseException:
    jsp.query("DROP TABLE `stations_temp`")

# Close Jasper instance
jsp.close()
