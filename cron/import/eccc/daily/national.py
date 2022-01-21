"""
ECCC national daily data import routine

Get daily data for weather stations in Canada.

The code is licensed under the MIT license.
"""

from datetime import datetime
from multiprocessing.pool import ThreadPool
import pandas as pd
from meteor import Meteor, run
from meteor.schema import daily_national


class Task(Meteor):
    """
    Import daily (national) data from ECCC
    """

    name = 'import.eccc.daily.national'  # Task name
    # dev_mode = True  # Running in dev mode?

    # Number of threads for parallel processing
    THREADS = 8
    # Base URL of ECCC interface
    BASE_URL = (
        'https://climate.weather.gc.ca/climate_data/bulk_data_e.html' +
        '?format=csv&timeframe=2&submit=Download+Data'
    )
    # Start year
    FIRST_YEAR = 2000
    # Current year
    CURRENT_YEAR = datetime.now().year
    # How many stations per cycle?
    STATIONS_PER_CYCLE = 1
    # Which parameters should be included?
    PARAMETERS = {
        'Max Temp (°C)': 'tmax',
        'Min Temp (°C)': 'tmin',
        'Mean Temp (°C)': 'tavg',
        'Total Precip (mm)': 'prcp',
        'Snow on Grnd (cm)': 'snow',
        'Spd of Max Gust (km/h)': 'wpgt'
    }

    def _load(self, station: str, year: int) -> pd.DataFrame:
        """
        Load dataset into DataFrame
        """
        try:
            # CSV URL
            url = f'{self.BASE_URL}&stationID={station["national_id"]}&Year={str(year)}'
            # Read into DataFrame
            df = pd.read_csv(url, parse_dates={'time': [4]})
            # Rename columns
            df = df.rename(self.PARAMETERS, axis=1)
            # Remove obsolete columns
            df = df[df.columns.intersection(
                ['time', *list(self.PARAMETERS.values())])]
            # Add station column
            df['station'] = station['id']

            # Snow cm to mm
            df['snow'] = df['snow'] * 10

            # Return DataFrame
            return df

        except BaseException:
            return pd.DataFrame()

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get some stations
        stations = self.get_stations("""
            SELECT
                `id`,
                `national_id`,
                `altitude`
            FROM
                `stations`
            WHERE
                `country` = 'CA' AND
                `national_id` IS NOT NULL
        """, self.STATIONS_PER_CYCLE)

        # List of datasets
        datasets = []

        # Go through all stations
        if len(stations) > 0:
            for station in stations:
                # Go through all years
                for year in range(self.FIRST_YEAR, self.CURRENT_YEAR + 1):
                    datasets.append([station, year])

        # Multi-thread processing
        if len(datasets) > 1:

            # Create process pool
            with ThreadPool(self.THREADS) as pool:
                # Process datasets in pool
                output = pool.starmap(self._load, datasets)
                # Wait for Pool to finish
                pool.close()
                pool.join()

            # DataFrame structure
            base = pd.DataFrame(columns=list(self.PARAMETERS.values()))

            # Full DataFrame
            full = pd.concat([base, *output])

            # Set index
            full.set_index(['station', 'time'], inplace=True)

            # Drop NaN-only rows
            full = full.dropna(how='all')

            if full.index.size > 0:
                # Write into database
                self.persist(full, daily_national)


run(Task)
