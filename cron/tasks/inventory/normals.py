"""
Update normals inventory

The code is licensed under the MIT license.
"""

from meteostat import Normals
from meteor import Meteor, run

Normals.max_age = 0


class Task(Meteor):
    """
    Update climate normals inventory data
    """

    name = 'task.inventory.normals'  # Task name

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
                    # Get daily data from Meteostat
                    data = Normals(station[0], 'all').fetch()

                    # Get start & end dates of time series
                    start = data.index.get_level_values('start').min()
                    end = data.index.get_level_values('end').max()
                    start = f'{start}-01-01'
                    end = f'{end}-12-31'

                    if len(start) == 10 and len(end) == 10:
                        self.query(f'''
                            INSERT INTO `inventory`(
                                `station`,
                                `mode`,
                                `start`,
                                `end`
                            ) VALUES (
                                "{station[0]}",
                                "N",
                                "{start}",
                                "{end}"
                            )
                            ON DUPLICATE KEY UPDATE
                                `start` = VALUES(`start`),
                                `end` = VALUES(`end`)
                        ''')

                except BaseException:
                    pass


run(Task)
