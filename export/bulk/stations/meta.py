"""
Export meta data for weather stations

The code is licensed under the MIT license.
"""

from io import BytesIO, StringIO
from gzip import GzipFile
import csv
import json
from routines import Routine

task = Routine('export.bulk.stations.meta', True)

def write_json_dump(data: list, name: str) -> None:

    global task

    file = BytesIO()

    if len(data) > 0:

        with GzipFile(fileobj=file, mode='w') as gz:
            gz.write(json.dumps(data, indent=4, default=str, ensure_ascii=False).encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.storbinary(f'STOR /stations/{name}.json.gz', file)

def write_csv_dump(data: list, name: str) -> None:

    global task

    if len(data) > 0:

        file = BytesIO()

        with GzipFile(fileobj=file, mode='w') as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=',')
            writer.writerows(data)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.storbinary(f'STOR /stations/{name}.csv.gz', file)

# Export data for all weather stations
result = task.read(f'''
    SELECT
        `stations`.`id` AS `id`,
        `stations`.`name` AS `name`,
        `stations`.`name_alt` AS `name_alt`,
        `stations`.`country` AS `country`,
        `stations`.`region` AS `region`,
        `stations`.`national_id` AS `national_id`,
        CAST(`stations`.`wmo` AS CHAR(5)) AS `wmo`,
        `stations`.`icao` AS `icao`,
        `stations`.`latitude` AS `latitude`,
        `stations`.`longitude` AS `longitude`,
        `stations`.`altitude` AS `altitude`,
        `stations`.`tz` as `timezone`,
        `stations`.`history` as `history`,
        MIN(`inventory_model`.`start`) AS "model_start",
        MAX(`inventory_model`.`end`) AS "model_end",
        MIN(`inventory_hourly`.`start`) AS "hourly_start",
        MAX(`inventory_hourly`.`end`) AS "hourly_end",
        MIN(`inventory_daily`.`start`) AS "daily_start",
        MAX(`inventory_daily`.`end`) AS "daily_end",
        YEAR(MIN(`inventory_monthly`.`start`)) AS "monthly_start",
        YEAR(MAX(`inventory_monthly`.`end`)) AS "monthly_end",
        YEAR(MIN(`inventory_normals`.`start`)) AS "normals_start",
        YEAR(MAX(`inventory_normals`.`end`)) AS "normals_end"
    FROM `stations`
    LEFT JOIN (
        SELECT
            `station`,
            `start`,
            `end`
        FROM `inventory`
        WHERE
            `mode` = "P"
    )
    AS
        `inventory_model`
    ON
        `stations`.`id` = `inventory_model`.`station`
    LEFT JOIN (
        SELECT
            `station`,
            `start`,
            `end`
        FROM `inventory`
        WHERE
            `mode` = "H"
    )
    AS
        `inventory_hourly`
    ON
        `stations`.`id` = `inventory_hourly`.`station`
    LEFT JOIN (
        SELECT
            `station`,
            `start`,
            `end`
        FROM `inventory`
        WHERE
            `mode` = "D"
    )
    AS
        `inventory_daily`
    ON
        `stations`.`id` = `inventory_daily`.`station`
    LEFT JOIN (
        SELECT
            `station`,
            `start`,
            `end`
        FROM `inventory`
        WHERE
            `mode` = "M"
    )
    AS
        `inventory_monthly`
    ON
        `stations`.`id` = `inventory_monthly`.`station`
    LEFT JOIN (
        SELECT
            `station`,
            `start`,
            `end`
        FROM `inventory`
        WHERE
            `mode` = "N"
    )
    AS
        `inventory_normals`
    ON
        `stations`.`id` = `inventory_normals`.`station`
    GROUP BY
        `stations`.`id`
''')

if result.rowcount > 0:

    # Fetch data
    data = result.fetchall()

    # Data lists
    full = []
    lite = []
    slim = []

    for record in data:

        # Create dict of names
        try:
            names = json.loads(record[2])
        except BaseException:
            names = {}
        names['en'] = record[1]

        # Create object
        object = {
            'id': record[0],
            'name': names,
            'country': record[3],
            'region': record[4],
            'identifiers': {
                'national': record[5],
                'wmo': record[6],
                'icao': record[7]
            },
            'location': {
                'latitude': record[8],
                'longitude': record[9],
                'elevation': record[10]
            },
            'timezone': record[11],
            'inventory': {
                'model': {
                    'start': record[13],
                    'end': record[14]
                },
                'hourly': {
                    'start': record[15],
                    'end': record[16]
                },
                'daily': {
                    'start': record[17],
                    'end': record[18]
                },
                'monthly': {
                    'start': record[19],
                    'end': record[20]
                },
                'normals': {
                    'start': record[21],
                    'end': record[22]
                }
            }
        }

        # Add to full dump
        full.append(object)

        # Check if any data is available
        if record[14] is not None or record[16] is not None or record[18] is not None or record[20] is not None or record[22] is not None:
            lite.append(object)
            # Convert to list
            record = record.values()
            # Add slim rows
            slim_cols = [0, 1, 3, 4, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20]
            slim.append([record[i] for i in slim_cols])

    # Write JSON dumps
    write_json_dump(full, 'full')
    write_json_dump(lite, 'lite')

    # Write CSV dumps
    write_csv_dump(slim, 'slim')
    write_csv_dump([row[0:13] for row in slim], 'lib')
