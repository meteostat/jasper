"""
Met.no hourly model import routine

Get hourly model forecasts for weather stations based on geo location

The code is licensed under the MIT license.
"""

from urllib import request
import json
import pandas as pd
from routines import Routine
from routines.schema import hourly_model

# Configuration
STATIONS_PER_CYCLE = 20

importer = Routine('import.metno.hourly.model')

stations = importer.get_stations("""
    SELECT
        `stations`.`id` AS `id`,
        `stations`.`latitude` AS `latitude`,
        `stations`.`longitude` AS `longitude`,
        `stations`.`altitude` AS `altitude`
    FROM `stations`
    WHERE
        `stations`.`latitude` IS NOT NULL AND
        `stations`.`longitude` IS NOT NULL AND
        `stations`.`altitude` IS NOT NULL AND
        `stations`.`mosmix` IS NULL AND
        `stations`.`id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
            WHERE
                `mode` IN ('H', 'D') AND
                `end` > DATE_SUB(NOW(), INTERVAL 1095 DAY)
        )
""", STATIONS_PER_CYCLE)

# Import data for each weather station
for station in stations:

    # Create request for JSON file
    url = f"https://api.met.no/weatherapi/locationforecast/2.0/complete.json?altitude={station[3]}&lat={station[1]}&lon={station[2]}"
    req = request.Request(
        url,
        headers={
            'User-Agent': 'meteostat.net info@meteostat.net'
        }
    )

    # Get JSON data
    with request.urlopen(req) as raw:
        data = json.loads(raw.read().decode())

    # Map JSON data
    def map_data(record):
        return {
            'time': record['time'],
            'temp': record['data']['instant']['details']['air_temperature'] if 'air_temperature' in record['data']['instant']['details'] else None,
			'rhum': record['data']['instant']['details']['relative_humidity'] if 'relative_humidity' in record['data']['instant']['details'] else None,
            'prcp': record['data']['next_1_hours']['details']['precipitation_amount'] if 'next_1_hours' in record['data'] and 'precipitation_amount' in record['data']['next_1_hours']['details'] else None,
			'wspd': record['data']['instant']['details']['wind_speed'] if 'wind_speed' in record['data']['instant']['details'] else None,
			'wpgt': record['data']['instant']['details']['wind_speed_of_gust'] if 'wind_speed_of_gust' in record['data']['instant']['details'] else None,
			'wdir': record['data']['instant']['details']['wind_from_direction'] if 'wind_from_direction' in record['data']['instant']['details'] else None,
			'pres': record['data']['instant']['details']['air_pressure_at_sea_level']  if 'air_pressure_at_sea_level' in record['data']['instant']['details'] else None

        }

    # Create DataFrame
    df = pd.DataFrame(map(map_data, data['properties']['timeseries']))

    # Set index
    df['station'] = station[0]
    df = df.set_index(['station', 'time'])

    # Shift prcp column by 1 (as it refers to the next hour)
    df['prcp'] = df['prcp'].shift(1)

    # Write DataFrame into Meteostat database
    importer.write(df, hourly_model)
