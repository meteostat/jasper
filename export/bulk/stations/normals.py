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
from meteostat import Monthly
from routines import Routine

# Configuration
Monthly.max_age = 0
STATIONS_PER_CYCLE = 10

task = Routine('export.bulk.normals', True)

stations = task.get_stations(f'''
    SELECT
        `stations`.`id` AS `id`
    FROM `stations`
    WHERE
        `stations`.`id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
        )
''', STATIONS_PER_CYCLE)

stations = [['10637']]

# Export data for each weather station
for station in stations:

    # Full DataFrame
    data: pd.DataFrame = pd.DataFrame(columns=[
        'start',
        'end',
        'month',
        'tavg',
        'tmin',
        'tmax',
        'prcp',
        'wspd',
        'pres',
        'tsun'
    ])

    # Get centuries
    decades = []
    loop_year = 1990
    current_year = datetime.now().year
    while loop_year < current_year:
        decades.append(loop_year)
        loop_year += 10

    # Collect normals from Monthly interface
    for year in decades:

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
        df.index.rename('month', inplace=True)
        df = df.reset_index()
        df['start'] = year - 29
        df['end'] = year
        df = df.set_index(['start', 'end', 'month'])
        # Remove uncertain data
        for parameter in coverage:
            if parameter in df.columns and coverage[parameter] < 0.6:
                df[parameter] = np.NaN

        data = data.append(df)

    # Get normals from DB
    data_db = pd.read_sql(f'''
		SET STATEMENT
			max_statement_time=60
		FOR
		SELECT
            `start`,
            `end`,
            `month`,
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
    index_col=['start', 'end', 'month'])

    # Merge data
    if data_db.index.size > 0:
        data = data.merge(data_db, how='outer', on=['start', 'end', 'month'])

    # Convert to list
    data = data.values.tolist()

    if len(data) > 0:

        file = BytesIO()

        with GzipFile(fileobj=file, mode='w') as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=',')
            writer.writerows(data)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.cwd(f'/normals')
        task.bulk_ftp.storbinary(f'STOR {station[0]}.csv.gz', file)
