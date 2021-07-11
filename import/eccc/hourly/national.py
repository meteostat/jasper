"""
ECCC national hourly data import routine

Get hourly data for weather stations in Canada.

The code is licensed under the MIT license.
"""

from datetime import datetime
from multiprocessing.pool import ThreadPool
import pandas as pd
from routines import Routine
from routines.convert import pres_to_msl
from routines.schema import hourly_national

# Number of threads for parallel processing
THREADS = 8
# Base URL of ECCC interface
BASE_URL = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=UTC&timeframe=1&submit=Download+Data'
# Start year
FIRST_YEAR = 2000
# Current year
CURRENT_YEAR = datetime.now().year
# How many stations per cycle?
STATIONS_PER_CYCLE = 1
# Which parameters should be included?
PARAMETERS = {
    'Temp (°C)': 'temp',
    'Rel Hum (%)': 'rhum',
    'Precip. Amount (mm)': 'prcp',
    'Wind Dir (10s deg)': 'wdir',
    'Wind Spd (km/h)': 'wspd',
    'Stn Press (kPa)': 'pres'
}

# Create task
task = Routine('import.eccc.hourly.national')

def _load(station: str, year: int, month: int):
    """
    Load dataset into DataFrame
    """

    try:

        # CSV URL
        url = f'{BASE_URL}&stationID={station["national_id"]}&Year={str(year)}&Month={str(month)}'
        # Read into DataFrame
        df = pd.read_csv(url, parse_dates={'time': [4]})
        # Rename columns
        df = df.rename(PARAMETERS, axis=1)
        # Remove obsolete columns
        df = df[df.columns.intersection(['time', *list(PARAMETERS.values())])]
        # Add station column
        df['station'] = station['id']

        # Wind direction to degrees
        df['wdir'] = df['wdir'] * 10
        # Convert PRES to MSL
        df['pres'] = df['pres'] * 10
        df['pres'] = df.apply(
            lambda row: pres_to_msl(
                row, station['altitude'], 'temp'), axis=1)

        # Return DataFrame
        return df

    except BaseException:

        pass

# Get some stations
stations = task.get_stations("""
    SELECT
        `id`,
        `national_id`,
        `altitude`
    FROM
        `stations`
    WHERE
        `country` = 'CA' AND
        `national_id` IS NOT NULL
""", STATIONS_PER_CYCLE)

# List of datasets
datasets = []

# Go through all stations
if len(stations) > 0:
    for station in stations:
        # Go through all years
        for year in range(FIRST_YEAR, CURRENT_YEAR + 1):
            # Go through all months
            for month in range(1, 13):
                datasets.append([station, year, month])

# Multi-thread processing
if len(datasets) > 1:

    # Create process pool
    with ThreadPool(THREADS) as pool:
        # Process datasets in pool
        output = pool.starmap(_load, datasets)
        # Wait for Pool to finish
        pool.close()
        pool.join()

    # DataFrame structure
    base = pd.DataFrame(columns=list(PARAMETERS.values()))

    # Full DataFrame
    full = pd.concat([base, *output])

    # Set index
    full.set_index(['station', 'time'], inplace=True)

    # Drop NaN-only rows
    full = full.dropna(how='all')

    if full.index.size > 0:
        # Write into database
        task.write(full, hourly_national)
