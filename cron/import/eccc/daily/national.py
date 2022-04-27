"""
ECCC national daily data import routine

Get daily data for weather stations in Canada.

The code is licensed under the MIT license.
"""

from datetime import datetime
from multiprocessing.pool import ThreadPool
import pandas as pd
from jasper import Jasper
from jasper.actions import persist
from jasper.helpers import get_stations
from jasper.schema import daily_national


# General configuratiob
THREADS = 8  # Number of threads for parallel processing
# Base URL of ECCC interface
BASE_URL = (
    "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
    + "?format=csv&timeframe=2&submit=Download+Data"
)
FIRST_YEAR = 2000  # Start year
CURRENT_YEAR = datetime.now().year  # Current year
STATIONS_PER_CYCLE = 1  # How many stations per cycle?
# Which parameters should be included?
PARAMETERS = {
    "Max Temp (°C)": "tmax",
    "Min Temp (°C)": "tmin",
    "Mean Temp (°C)": "tavg",
    "Total Precip (mm)": "prcp",
    "Snow on Grnd (cm)": "snow",
    "Spd of Max Gust (km/h)": "wpgt",
}

# Create Jasper instance
jsp = Jasper("import.eccc.daily.national")


def load(station: str, year: int) -> pd.DataFrame:
    """
    Load dataset into DataFrame
    """
    try:
        # CSV URL
        url = f'{BASE_URL}&stationID={station["national_id"]}&Year={str(year)}'
        # Read into DataFrame
        df = pd.read_csv(url, parse_dates={"time": [4]})
        # Rename columns
        df = df.rename(PARAMETERS, axis=1)
        # Remove obsolete columns
        df = df[df.columns.intersection(["time", *list(PARAMETERS.values())])]
        # Add station column
        df["station"] = station["id"]

        # Snow cm to mm
        df["snow"] = df["snow"] * 10

        # Return DataFrame
        return df

    except BaseException:
        return pd.DataFrame()


# Get some stations
stations = get_stations(
    jsp,
    """
    SELECT
        `id`,
        `national_id`,
        `altitude`
    FROM
        `stations`
    WHERE
        `country` = 'CA' AND
        `national_id` IS NOT NULL
    """,
    STATIONS_PER_CYCLE,
)

# List of datasets
datasets = []

# Go through all stations
if len(stations) > 0:
    for station in stations:
        # Go through all years
        for year in range(FIRST_YEAR, CURRENT_YEAR + 1):
            datasets.append([station, year])

# Multi-thread processing
if len(datasets) > 1:

    # Create process pool
    with ThreadPool(THREADS) as pool:
        # Process datasets in pool
        output = pool.starmap(load, datasets)
        # Wait for Pool to finish
        pool.close()
        pool.join()

    # DataFrame structure
    base = pd.DataFrame(columns=list(PARAMETERS.values()))

    # Full DataFrame
    full = pd.concat([base, *output])

    # Set index
    full.set_index(["station", "time"], inplace=True)

    # Drop NaN-only rows
    full = full.dropna(how="all")

    if full.index.size > 0:
        # Write into database
        persist(jsp, full, daily_national)

# Close Jasper instance
jsp.close()
