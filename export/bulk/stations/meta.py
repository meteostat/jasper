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
            gz.write(json.dumps(data, indent=4, default=str).encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.storbinary(f'STOR /stations/meta/{name}.json.gz', file)

# Export data for all weather stations
result = task.read(f'''
    SELECT
        `stations`.`id` AS `id`,
        `stations`.`name` AS `name`,
        `stations`.`name_alt` AS `name_alt`,
        `stations`.`region` AS `region`,
        `stations`.`country` AS `country`,
        `stations`.`national_id` AS `national_id`,
        `stations`.`wmo` AS `wmo`,
        `stations`.`icao` AS `icao`,
        `stations`.`iata` AS `iata`,
        `stations`.`latitude` AS `latitude`,
        `stations`.`longitude` AS `longitude`,
        `stations`.`altitude` AS `altitude`,
        `stations`.`tz` as `timezone`,
        `stations`.`history` as `history`,
        COUNT(`inventory`.`mode`) AS `availability`,
        MIN(CASE WHEN `inventory`.`mode` = "H" THEN `inventory`.`start` END) AS "hourly_start",
        MAX(CASE WHEN `inventory`.`mode` = "H" THEN `inventory`.`end` END) AS "hourly_end",
        MIN(CASE WHEN `inventory`.`mode` = "P" THEN `inventory`.`start` END) AS "model_start",
        MAX(CASE WHEN `inventory`.`mode` = "P" THEN `inventory`.`end` END) AS "model_end",
        MIN(CASE WHEN `inventory`.`mode` = "D" THEN `inventory`.`start` END) AS "daily_start",
        MAX(CASE WHEN `inventory`.`mode` = "D" THEN `inventory`.`end` END) AS "daily_end"
    FROM `stations`
    LEFT JOIN `inventory`
    ON `stations`.`id` = `inventory`.`station`
    GROUP BY `stations`.`id`
''')

if result.rowcount > 0:

    # Fetch data
    data = result.fetchall()

    # Data lists
    full = []
    lite = []
    lib = []

    for record in data:

        # Create dict of names
        try:
            names = json.loads(data[2])
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
                'icao': record[7],
                'iata': record[8]
            },
            'location': {
                'latitude': record[9],
                'longitude': record[10],
                'elevation': record[11]
            },
            'timezone': record[12],
            'history': record[13],
            'inventory': {
                'hourly': {
                    'start': record[15],
                    'end': record[16]
                },
                'model': {
                    'start': record[17],
                    'end': record[18]
                },
                'daily': {
                    'start': record[19],
                    'end': record[20]
                }
            }
        }

        # Add to full dump
        full.append(object)

        # Check if any data is available
        if record[14] > 0:
            lite.append(object)
            # Add CSV row
            record = record.values()
            del record[2]
            del record[5]
            del record[8]
            del record[13]
            del record[14]
            lib.append(record)

    # Write JSON dumps
    write_json_dump(full, 'full')
    write_json_dump(lite, 'lite')

    # Write CSV dump
    if len(lib) > 0:

        file = BytesIO()

        with GzipFile(fileobj=file, mode='w') as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=',')
            writer.writerows(lib)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.storbinary(f'STOR /stations/meta/lib.csv.gz', file)
