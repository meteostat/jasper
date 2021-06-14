"""
Update monthly inventory

The code is licensed under the MIT license.
"""

from routines import Routine
from meteostat import Monthly

# Configuration
Monthly.max_age = 0
STATIONS_PER_CYCLE = 10

# Create routine
task = Routine('task.inventory.monthly')

# Get stations
stations = task.get_stations("""
    SELECT
        `stations`.`id` AS `id`
    FROM
        `stations`
    WHERE
        `stations`.`id` IN (
            SELECT DISTINCT
                `station`
            FROM
                `inventory`
            WHERE
                `mode` IN ('H', 'D', 'M')
        )
""", STATIONS_PER_CYCLE)

if len(stations) > 0:

    for station in stations:

        try:

            # Get monthly data from Meteostat
            data = Monthly(station[0], model=False).fetch()

            # Get start & end dates of time series
            start = data.index.min().strftime('%Y-%m-%d')
            end = data.index.max().strftime('%Y-%m-%d')

            if len(start) == 10 and len(end) == 10:

                task.query(f'''
                    INSERT INTO `inventory`(
                        `station`,
                        `mode`,
                        `start`,
                        `end`
                    ) VALUES (
                        "{station[0]}",
                        "M",
                        "{start}",
                        "{end}"
                    )
                    ON DUPLICATE KEY UPDATE
                        `start` = VALUES(`start`),
                        `end` = VALUES(`end`)
                ''')

        except BaseException:

            pass
