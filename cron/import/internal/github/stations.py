"""
Get latest weather station meta data from GitHub

The code is licensed under the MIT license.
"""

import json
import re
import urllib.request as request
import zipfile
from meteor import Meteor, run


class Task(Meteor):
    """
    Sync weather stations between GitHub and Meteostat DB
    """

    name = 'import.internal.github.stations'  # Task name
    # dev_mode = True  # Running in dev mode?

    # pylint: disable=inconsistent-return-statements
    def write_station(self, data: dict) -> None:
        """
        Add a new weather station or update existing
        """
        try:
            # Break if invalid data
            if len(data['id']) != 5 or len(data['country']) != 2:
                return None

            # Extract alternate names
            name_alt = {
                key: data['name'][key] for key in data['name'] if key != 'en'
            }

            self.query('''
                INSERT INTO `stations_temp` (
                    `id`,
                    `country`,
                    `region`,
                    `name`,
                    `name_alt`,
                    `national_id`,
                    `wmo`,
                    `icao`,
                    `iata`,
                    `ghcn`,
                    `wban`,
                    `usaf`,
                    `mosmix`,
                    `latitude`,
                    `longitude`,
                    `altitude`,
                    `tz`
                )
                VALUES (
                    :id,
                    :country,
                    :region,
                    :name,
                    :name_alt,
                    :national_id,
                    :wmo,
                    :icao,
                    :iata,
                    :ghcn,
                    :wban,
                    :usaf,
                    :mosmix,
                    :lat,
                    :lon,
                    :elevation,
                    :tz
                )
                ON DUPLICATE KEY UPDATE
                    `id` = VALUES(`id`),
                    `country` = VALUES(`country`),
                    `region` = VALUES(`region`),
                    `name` = VALUES(`name`),
                    `name_alt` = VALUES(`name_alt`),
                    `national_id` = VALUES(`national_id`),
                    `wmo` = VALUES(`wmo`),
                    `icao` = VALUES(`icao`),
                    `iata` = VALUES(`iata`),
                    `ghcn` = VALUES(`ghcn`),
                    `wban` = VALUES(`wban`),
                    `usaf` = VALUES(`usaf`),
                    `mosmix` = VALUES(`mosmix`),
                    `latitude` = VALUES(`latitude`),
                    `longitude` = VALUES(`longitude`),
                    `altitude` = VALUES(`altitude`),
                    `tz` = VALUES(`tz`)
            ''', {
                'id': data['id'],
                'country': data['country'],
                'region': data['region'],
                'name': data['name']['en'],
                'name_alt': json.dumps(name_alt, ensure_ascii=False, sort_keys=True),
                'national_id': data['identifiers']['national'] if (
                    'national' in data['identifiers']
                ) else None,
                'wmo': data['identifiers']['wmo'] if 'wmo' in data['identifiers'] else None,
                'icao': data['identifiers']['icao'] if 'icao' in data['identifiers'] else None,
                'iata': data['identifiers']['iata'] if 'iata' in data['identifiers'] else None,
                'ghcn': data['identifiers']['ghcn'] if 'ghcn' in data['identifiers'] else None,
                'wban': data['identifiers']['wban'] if 'wban' in data['identifiers'] else None,
                'usaf': data['identifiers']['usaf'] if 'usaf' in data['identifiers'] else None,
                'mosmix': data['identifiers']['mosmix'] if 'mosmix' in data['identifiers'] else None,
                'lat': data['location']['latitude'],
                'lon': data['location']['longitude'],
                'elevation': data['location']['elevation'],
                'tz': data['timezone']
            })

        except BaseException:
            pass

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Create copy of stations table
        self.query('CREATE TABLE `stations_temp` LIKE `stations`')

        try:
            # Load station repository
            handle, _ = request.urlretrieve(
                'https://github.com/meteostat/weather-stations/archive/refs/heads/master.zip'
            )
            zip_obj = zipfile.ZipFile(handle, 'r')

            # Write all stations
            for index, name in enumerate(zip_obj.namelist()):
                if re.search("/stations/([A-Z0-9]{5}).json$", name):
                    file = zip_obj.namelist()[index]
                    raw = zip_obj.open(file)
                    data = json.loads(raw.read().decode('UTF-8'))
                    self.write_station(data)

            # Remove existing table
            self.query('DROP TABLE `stations`')

            # Rename temp table
            self.query('ALTER TABLE `stations_temp` RENAME `stations`')

        except BaseException:
            self.query('DROP TABLE `stations_temp`')


run(Task)
