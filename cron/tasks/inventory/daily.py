"""
Update daily inventory

The code is licensed under the MIT license.
"""

from datetime import datetime
from meteostat import Daily
from meteor import Meteor, run


class Task(Meteor):
    """
    Update daily inventory data
    """

    name = 'task.inventory.daily'  # Task name

    Daily.max_age = 0
    STATIONS_PER_CYCLE = 10

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get stations
        stations = self.get_stations('''
            SELECT
                `id`
            FROM
                `stations`
        ''', self.STATIONS_PER_CYCLE)

        if len(stations) > 0:
            for station in stations:
                try:
                    # Get daily data from Meteostat
                    data = Daily(station[0], model=False).fetch()

                    # Get start & end dates of time series
                    start = data.index.min().strftime('%Y-%m-%d')
                    end = data.index.max().strftime('%Y-%m-%d')

                    if len(start) == 10 and len(end) == 10:

                        self.query(f'''
                            INSERT INTO `inventory`(
                                `station`,
                                `mode`,
                                `start`,
                                `end`
                            ) VALUES (
                                "{station[0]}",
                                "D",
                                "{start}",
                                "{end}"
                            )
                            ON DUPLICATE KEY UPDATE
                                `start` = VALUES(`start`),
                                `end` = VALUES(`end`)
                        ''')

                except BaseException:
                    pass

        # Get current time
        now = datetime.now()

        # Run daily
        if now.hour == 3 and now.minute == 20:
            self.query('''
                INSERT INTO
                    `inventory`(`station`, `mode`, `start`)
                SELECT
                    `station`,
                    'D' AS `mode`,
                    MIN(`mindate`) AS `start` FROM (
                        (SELECT
                            `station`,
                            MIN(`date`) as `mindate`
                        FROM `daily_national`
                        GROUP BY `station`)
                    UNION ALL
                        (SELECT
                            `station`,
                            MIN(`date`) as `mindate`
                        FROM `daily_ghcn`
                        GROUP BY `station`)
                    ) AS `daily_inventory`
                GROUP BY `station`
                ON DUPLICATE KEY UPDATE
                    `start` = COALESCE(LEAST(VALUES(`start`), `start`), VALUES(`start`), `start`)
            ''')

            self.query('''
                INSERT INTO
                    `inventory`(`station`, `mode`, `end`)
                SELECT
                    `station`,
                    'D' AS `mode`,
                    MAX(`maxdate`) AS `end` FROM (
                        (SELECT
                            `station`,
                            MAX(`date`) as `maxdate`
                        FROM `daily_national`
                        GROUP BY `station`)
                    UNION ALL
                        (SELECT
                            `station`,
                            MAX(`date`) as `maxdate`
                        FROM `daily_ghcn`
                        GROUP BY `station`)
                    ) AS `daily_inventory`
                GROUP BY `station`
                ON DUPLICATE KEY UPDATE
                    `end` = COALESCE(GREATEST(VALUES(`end`), `end`), VALUES(`end`), `end`)
            ''')

            # Legacy
            # pylint: disable=line-too-long
            self.query("INSERT INTO `stations_inventory`(`station`, `daily_start`) SELECT `station`,MIN(`mindate`) AS `daily_start` FROM ((SELECT `station`,MIN(`date`) as `mindate` FROM `daily_national` GROUP BY `station`) UNION ALL (SELECT `station`,MIN(`date`) as `mindate` FROM `daily_ghcn` GROUP BY `station`)) AS `daily_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `daily_start` = VALUES(`daily_start`)")
            self.query("INSERT INTO `stations_inventory`(`station`, `daily_end`) SELECT `station`,MAX(`maxdate`) AS `daily_end` FROM ((SELECT `station`,MAX(`date`) as `maxdate` FROM `daily_national` GROUP BY `station`) UNION ALL (SELECT `station`,MAX(`date`) as `maxdate` FROM `daily_ghcn` GROUP BY `station`)) AS `daily_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `daily_end` = VALUES(`daily_end`)")


run(Task)
