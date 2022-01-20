"""
Export daily bulk data

The code is licensed under the MIT license.
"""

import os
from meteor import Meteor, run


class Task(Meteor):
    """
    Export daily weather station data
    """

    name = 'export.bulk.daily'  # Task name
    use_bulk = True  # Connect to Meteostat Bulk server?
    # dev_mode = True # Run task in dev mode?

    STATIONS_PER_CYCLE = 10
    FLAGS = {
        'A': 'N',  # National dataset
        'B': 'G',  # Global datasat
        'C': 'H',  # Processed national dataset
        'D': 'H',  # Processed global dataset
        'E': 'H',  # Processed SYNOP data
        'F': 'H',  # Processed METAR data
        'G': 'M'  # Processed model data
    }

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get weather station(s)
        stations = self.get_stations('''
            SELECT
                `stations`.`id` AS `id`,
                `stations`.`tz` AS `tz`
            FROM `stations`
            WHERE
                `stations`.`id` IN (
                    SELECT DISTINCT `station`
                    FROM `inventory`
                    WHERE
                        `mode` IN ('D', 'H', 'P')
                )
        ''', self.STATIONS_PER_CYCLE)

        # Export data for each weather station
        for station in stations:
            with open(
                f'{os.path.dirname(os.path.realpath(__file__))}/daily.sql',
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
                        lambda d: d[:11],
                        data
                    )),
                    f'/daily/{station[0]}.csv.gz'
                )

                # Export source map
                # pylint: disable=consider-using-generator
                self.export_csv(
                    list(map(
                        lambda d: d[:1] + tuple(
                            [
                                flag if flag not in self.FLAGS else self.FLAGS[flag]
                                for flag in d[11:]
                            ]
                        ),
                        data
                    )),
                    f'/daily/{station[0]}.map.csv.gz'
                )


# Run task
run(Task)
