"""
Get latest weather station meta data from GitHub

The code is licensed under the MIT license.
"""

import os
import json
from routines import Routine

# Configuration
REPO_PATH = os.path.expanduser('~') + os.sep + 'repos' + os.sep + 'weather-stations'

# Create task
task = Routine('import.internal.github.stations')

"""
Get a JSON file
"""
def get_file(file: str) -> dict:

    with open(file, 'r') as f:
        return json.load(f)

"""
Delete a weather station
"""
def delete_station(station: str) -> None:

    global task

    task.query('''
        DELETE FROM
            `stations`
        WHERE
            `id` = :station
        LIMIT 1
    ''', {
        'station': station
    })

"""
Add a new weather station
"""
def add_station(station: str, file: str) -> None:

    global task

    # Get JSON data
    data = get_file(file)

    # Break if invalid data
    if len(data['id']) != 5 or len(data['country']) != 2:
        return None

    # Extract alternate names
    name_alt = {
        key: data['name'][key] for key in data['name'] if key != 'en'
    }

    task.query('''
        INSERT INTO
            `stations` (
                `id`,
                `country`,
                `region`,
                `name`,
                `name_alt`,
                `national_id`,
                `wmo`,
                `icao`,
                `iata`,
                `latitude`,
                `longitude`,
                `altitude`,
                `tz`,
                `history`
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
            :lat,
            :lon,
            :elevation,
            :tz,
            :history
        )
    ''', {
        'id': data['id'],
        'country': data['country'],
        'region': data['region'],
        'name': data['name']['en'],
        'name_alt': json.dumps(name_alt),
        'national_id': data['identifiers']['national'],
        'wmo': data['identifiers']['wmo'],
        'icao': data['identifiers']['icao'],
        'iata': data['identifiers']['iata'],
        'lat': data['location']['latitude'],
        'lon': data['location']['longitude'],
        'elevation': data['location']['elevation'],
        'tz': data['timezone'],
        'history': json.dumps(data['history'])
    })

"""
Update existing weather station
"""
def update_station(station: str, file: str) -> None:

    global task

    # Get JSON data
    data = get_file(file)

    # Break if invalid data
    if len(data['id']) != 5 or len(data['country']) != 2:
        return None

    # Extract alternate names
    name_alt = {
        key: data['name'][key] for key in data['name'] if key != 'en'
    }

    task.query('''
        UPDATE
            `stations`
        SET
            `id` = :id,
            `country` = :country,
            `region` = :region,
            `name` = :name,
            `name_alt` = :name_alt,
            `national_id` = :national_id,
            `wmo` = :wmo,
            `icao` = :icao,
            `iata` = :iata,
            `latitude` = :lat,
            `longitude` = :lon,
            `altitude` = :elevation,
            `tz` = :tz,
            `history` = :history
        WHERE
            `id` = :id
    ''', {
        'id': data['id'],
        'country': data['country'],
        'region': data['region'],
        'name': data['name']['en'],
        'name_alt': json.dumps(name_alt),
        'national_id': data['identifiers']['national'],
        'wmo': data['identifiers']['wmo'],
        'icao': data['identifiers']['icao'],
        'iata': data['identifiers']['iata'],
        'lat': data['location']['latitude'],
        'lon': data['location']['longitude'],
        'elevation': data['location']['elevation'],
        'tz': data['timezone'],
        'history': json.dumps(data['history'])
    })

# Fetch repository
os.system(f'cd {REPO_PATH} && git fetch')

# Get diff
diff = os.popen(f'cd {REPO_PATH} && git diff master origin/master --name-status').read()

# Merge changes
os.system(f'cd {REPO_PATH} && git merge origin/master')

for change in diff.splitlines():

    # Get performed action
    action = change[0]

    # Get station ID
    station = change[-10:-5]

    # Get file path
    file = REPO_PATH + os.sep + 'stations' + os.sep + station + '.json'

    # Delete station
    if action == 'D':
        delete_station(station)

    # New station
    if action == 'A':
        add_station(station, file)

    # Update station
    if action == 'M' or action == 'R':
        update_station(station, file)
