"""
Export normals bulk data

The code is licensed under the MIT license.
"""

from io import BytesIO, StringIO
from gzip import GzipFile
import csv
from routines import Routine

# Configuration
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
            WHERE
                `mode` IN ('N')
        )
''', STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:

    result = task.read(f'''
		SET STATEMENT
			max_statement_time=60
		FOR
		SELECT
            `station`,
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
            `station` = :station
        ORDER BY
            `start`,
            `end`,
            `month`
    ''', {
        'station': station[0]
    })

    if result.rowcount > 0:

        file = BytesIO()

        with GzipFile(fileobj=file, mode='w') as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=',')
            writer.writerows(result)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.cwd(f'/stations/normals')
        task.bulk_ftp.storbinary(f'STOR {station[0]}.csv.gz', file)
