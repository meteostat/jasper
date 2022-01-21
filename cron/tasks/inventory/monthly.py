"""
Update monthly inventory

The code is licensed under the MIT license.
"""

from meteostat import Monthly
from meteor import Meteor, run

Monthly.max_age = 0


class Task(Meteor):
    """
    Update monthly inventory data
    """

    name = 'task.inventory.monthly'  # Task name

    STATIONS_PER_CYCLE = 10

    def main(self) -> None:
        """
        Main script & entry point
        """
        stations = self.get_stations('''
            SELECT
                `id`
            FROM
                `stations`
        ''', self.STATIONS_PER_CYCLE)

        if len(stations) > 0:
            for station in stations:
                try:
                    # Get monthly data from Meteostat
                    data = Monthly(station[0], model=False).fetch()

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


# Run task
run(Task)
