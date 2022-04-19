"""
Get latest weather station meta data from GitHub

The code is licensed under the MIT license.
"""

import os
import json
from multiprocessing.pool import ThreadPool
from meteor import Meteor, run


class Task(Meteor):
    """
    Sync weather stations between GitHub and Meteostat DB
    """

    name = 'import.internal.github.stations'  # Task name
    # dev_mode = True  # Running in dev mode?

    REPO_PATH = (
        os.path.expanduser('~') +
        os.sep +
        'repos' +
        os.sep +
        'weather-stations'
    )
    THREADS = 12

    @staticmethod
    def get_file(file: str) -> dict:
        """
        Get a JSON file
        """
        with open(file, 'r', encoding='UTF-8') as f:
            return json.load(f)

    def delete_station(self, station: str) -> None:
        """
        Delete a weather station
        """
        self.query('''
            DELETE FROM
                `stations`
            WHERE
                `id` = :station
            LIMIT 1
        ''', {
            'station': station
        })

    # pylint: disable=inconsistent-return-statements
    def write_station(self, file: str) -> None:
        """
        Add a new weather station or update existing
        """
        # Get JSON data
        data = self.get_file(file)

        # Break if invalid data
        if len(data['id']) != 5 or len(data['country']) != 2:
            return None

        # Extract alternate names
        name_alt = {
            key: data['name'][key] for key in data['name'] if key != 'en'
        }

        self.query('''
            INSERT INTO `stations` (
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
            'synoptic': data['identifiers']['synoptic'] if 'synoptic' in data['identifiers'] else None,
            'lat': data['location']['latitude'],
            'lon': data['location']['longitude'],
            'elevation': data['location']['elevation'],
            'tz': data['timezone']
        })

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Fetch repository
        os.system(f'cd {self.REPO_PATH} && git fetch')

        # Get diff
        diff = os.popen(
            f'cd {self.REPO_PATH} && git diff master origin/master --name-status'
        ).read()

        # Merge changes
        os.system(f'cd {self.REPO_PATH} && git merge origin/master')

        # Delete weather stations
        for change in diff.splitlines():
            # Get performed action
            action = change[0]

            # Delete station
            if action == 'D':

                # Get station ID
                station = change[-10:-5]

                # Delete station
                if action == 'D':
                    self.delete_station(station)

        # Update all weather stations
        files = []

        # Go through all files
        for dirpath, _dirnames, filenames in os.walk(
                self.REPO_PATH + os.sep + 'stations'):
            for filename in [f for f in filenames if f.endswith('.json')]:
                # Write station data
                files.append(os.path.join(dirpath, filename))

        # Create ThreadPool
        with ThreadPool(self.THREADS) as pool:
            # Process datasets in pool
            pool.map(self.write_station, files)
            # Wait for Pool to finish
            pool.close()
            pool.join()


run(Task)
