"""
Export normals bulk data

The code is licensed under the MIT license.
"""

from datetime import datetime
from io import BytesIO, StringIO
from gzip import GzipFile
import csv
import numpy as np
import pandas as pd
from sqlalchemy import text
from meteostat import Monthly
from routines import Routine

# Configuration
Monthly.max_age = 0
STATIONS_PER_CYCLE = 10

# Create routine
task = Routine('export.bulk.normals', True, True)

def get_bulk(station: list) -> pd.DataFrame:
    """
    Get climate normals from Meteostat Bulk interface
    """

    try:

        # Full DataFrame
        data: pd.DataFrame = pd.DataFrame()

        # Get decades
        decades = []
        loop_year = 1990
        current_year = datetime.now().year
        while loop_year < current_year:
            decades.append(loop_year)
            loop_year += 10

        # Collect normals from Monthly interface
        for year in decades:

            try:

                # Start & end year
                start = datetime(year-29, 1, 1)
                end = datetime(year, 12, 31)
                # Get data
                df = Monthly(station[0], start, end)
                # Get coverage data
                coverage = {}
                for parameter in df._columns[2:]:
                    coverage[parameter] = df.coverage(parameter)
                # Fetch DataFrame
                df = df.fetch()
                # Drop certain columns
                df = df.drop(['snow', 'wdir', 'wpgt'], axis=1)
                # Aggregate monthly
                df = df.groupby(df.index.month).agg('mean')
                df = df.round(1)
                # Refactor index
                df.reset_index(inplace=True)
                df = df.reset_index()
                df['start'] = year - 29
                df['end'] = year
                df.set_index(['start', 'end', 'time'], inplace=True)
                # Remove uncertain data
                for parameter in coverage:
                    if parameter in df.columns and coverage[parameter] < 0.6:
                        df[parameter] = np.NaN

                # Add to full DataFrame
                data = data.append(df)

            except BaseException:

                pass

        # Return full DataFrame
        return data

    except BaseException:

        return pd.DataFrame()

def get_worldclim(task, station: list) -> pd.DataFrame:
    """
    Load climate normals from WorldClim DB
    """

    try:

        # Collect normals from WorldClim
        with task.worldclim_db.connect() as con:
            tmin = con.execute(text(f'''
                SELECT
                    `altitude`,
                    `JAN`,
                    `FEB`,
                    `MAR`,
                    `APR`,
                    `MAY`,
                    `JUN`,
                    `JUL`,
                    `AUG`,
                    `SEP`,
                    `OCT`,
                    `NOV`,
                    `DEC`,
                    ROUND(
                        (6371*acos(cos(radians(:lat)) *
                        cos(radians(`latitude`)) *
                        cos(radians(`longitude`) -
                        radians(:lon)) +
                        sin(radians(:lat)) *
                        sin(radians(`latitude`)))
                    ), 1) AS `distance`
                FROM
                    `normals_temperature_min`
                WHERE
                    (`latitude` BETWEEN :lat-0.1 AND :lat+0.1) AND
                    (`longitude` BETWEEN :lon-0.1 AND :lon+0.1)
                HAVING
                    `distance` < 10
                ORDER BY
                    `distance`
                LIMIT
                    1
            '''), {
                'lat': station[1],
                'lon': station[2]
            })

            tmax = con.execute(text(f'''
                SELECT
                    `JAN`,
                    `FEB`,
                    `MAR`,
                    `APR`,
                    `MAY`,
                    `JUN`,
                    `JUL`,
                    `AUG`,
                    `SEP`,
                    `OCT`,
                    `NOV`,
                    `DEC`,
                    ROUND(
                        (6371*acos(cos(radians(:lat)) *
                        cos(radians(`latitude`)) *
                        cos(radians(`longitude`) -
                        radians(:lon)) +
                        sin(radians(:lat)) *
                        sin(radians(`latitude`)))
                    ), 1) AS `distance`
                FROM
                    `normals_temperature_max`
                WHERE
                    (`latitude` BETWEEN :lat-0.1 AND :lat+0.1) AND
                    (`longitude` BETWEEN :lon-0.1 AND :lon+0.1)
                HAVING
                    `distance` < 10
                ORDER BY
                    `distance`
                LIMIT
                    1
            '''), {
                'lat': station[1],
                'lon': station[2]
            })

            prcp = con.execute(text(f'''
                SELECT
                    `JAN`,
                    `FEB`,
                    `MAR`,
                    `APR`,
                    `MAY`,
                    `JUN`,
                    `JUL`,
                    `AUG`,
                    `SEP`,
                    `OCT`,
                    `NOV`,
                    `DEC`,
                    ROUND(
                        (6371*acos(cos(radians(:lat)) *
                        cos(radians(`latitude`)) *
                        cos(radians(`longitude`) -
                        radians(:lon)) +
                        sin(radians(:lat)) *
                        sin(radians(`latitude`)))
                    ), 1) AS `distance`
                FROM
                    `normals_precipitation`
                WHERE
                    (`latitude` BETWEEN :lat-0.1 AND :lat+0.1) AND
                    (`longitude` BETWEEN :lon-0.1 AND :lon+0.1)
                HAVING
                    `distance` < 10
                ORDER BY
                    `distance`
                LIMIT
                    1
            '''), {
                'lat': station[1],
                'lon': station[2]
            })

        # Fetch data
        tmin = [float(r) for r in tmin.fetchall()[0]]
        tmax = [float(r) for r in tmax.fetchall()[0]]
        prcp = [float(r) for r in prcp.fetchall()[0]]

        # Get grid cell altitude
        altitude = tmin.pop(0)

        # Calculate constant
        const = (2/3) * ((altitude - station[3]) / 100)

        # Create result list
        raw = []
        for i in range(12):
            raw.append({
                "time": i + 1,
                "tavg": round(((tmin[i] + tmax[i]) / 2) + const, 1),
                "tmin": round(tmin[i] + const, 1),
                "tmax": round(tmax[i] + const, 1),
                "prcp": round(prcp[i], 1)
            })

        # Convert to DataFrame
        df = pd.DataFrame(raw)

        # Set index
        df['start'] = 1961
        df['end'] = 1990
        df.set_index(['start', 'end', 'time'], inplace=True)

        # Return DataFrame
        return df

    except BaseException:

        return pd.DataFrame()

def get_database(task, station: list) -> pd.DataFrame:
    """
    Get climate normals from Meteostat DB
    """

    try:

        # Get data from DB
        df = pd.read_sql(f'''
    		SET STATEMENT
    			max_statement_time=60
    		FOR
    		SELECT
                `start`,
                `end`,
                `month` AS `time`,
                `tavg`,
                `tmin`,
                `tmax`,
                `prcp`,
                `pres`,
                `tsun`
            FROM
                `normals_global`
            WHERE
                `station` = "{station[0]}"
            ORDER BY
                `start`,
                `end`,
                `month`
        ''',
        task.db,
        index_col=['start', 'end', 'time'])

        # Return DataFrame
        return df

    except BaseException:

        return pd.DataFrame()


stations = task.get_stations(f'''
    SELECT
        `id`,
        `latitude`,
        `longitude`,
        `altitude`
    FROM
        `stations`
    WHERE
        `id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
        )
''', STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:

    try:

        # Get data
        bulk = get_bulk(station)
        worldclim = get_worldclim(task, station)
        database = get_database(task, station)

        # Merge data
        if bulk.index.size > 0 or worldclim.index.size > 0 or database.index.size > 0:
            data = pd.concat([bulk, worldclim, database])
            data = data.groupby([
                data.index.get_level_values('start'),
                data.index.get_level_values('end'),
                data.index.get_level_values('time')]
            ).agg('last')

        # Drop needless column(s)
        try:
            data = data.drop('index', axis=1)
        except BaseException:
            pass

        # Drop NaN-only rows
        data = data.dropna(how='all')
        data = data.sort_index()

        if data.index.size > 0:

            file = BytesIO()

            with GzipFile(fileobj=file, mode='w') as gz:
                output = StringIO()
                data.to_csv(output, header=False)
                gz.write(output.getvalue().encode())
                gz.close()
                file.seek(0)

            task.bulk_ftp.cwd(f'/normals')
            task.bulk_ftp.storbinary(f'STOR {station[0]}.csv.gz', file)

    except BaseException:

        pass
