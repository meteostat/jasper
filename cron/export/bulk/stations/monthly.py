"""
Export monthly bulk data

The code is licensed under the MIT license.
"""

import os
from meteor import Meteor, run


class Task(Meteor):
    """
    Export monthly station data
    """

    name = 'export.bulk.monthly'  # Task name
    use_bulk = True  # Connect to Meteostat Bulk server?
    # dev_mode = True  # Run task in dev mode?

    STATIONS_PER_CYCLE = 11

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get weather station(s)
        stations = self.get_stations('''
            SELECT
                `stations`.`id` AS `id`,
                `stations`.`tz` AS `tz`
            FROM
                `stations`
            WHERE
                `stations`.`id` IN (
                    SELECT DISTINCT
                        `station`
                    FROM
                        `inventory`
                    WHERE
                        `mode` IN ('M', 'D', 'H', 'P')
                )
        ''', self.STATIONS_PER_CYCLE)

        # Export data for each weather station
        for station in stations:
            with open(
                f'{os.path.dirname(os.path.realpath(__file__))}/monthly.sql',
                'r',
                encoding='UTF-8'
            ) as sql:
                result = self.query(sql.read(), {
                    'station': station[0],
                    'timezone': station[1]
                })
                sql.close()

            if result.rowcount > 0:
                # Fetch data
                data = result.fetchall()

                # Export data dump
                self.export_csv(
                    list(map(
                        lambda d: d[:12],
                        data
                    )),
                    f'/monthly/{station[0]}.csv.gz'
                )

                # Export source map
                # pylint: disable=consider-using-generator
                self.export_csv(
                    list(map(
                        lambda d: d[:2] + tuple(
                            [
                                ''.join(
                                    sorted(
                                        list(set(flag))
                                    )
                                ) if flag is not None else None for flag in d[12:]
                            ]
                        ),
                        data
                    )),
                    f'/monthly/{station[0]}.map.csv.gz'
                )


run(Task)
