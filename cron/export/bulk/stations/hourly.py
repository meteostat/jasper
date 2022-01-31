"""
Export hourly bulk data

The code is licensed under the MIT license.
"""

from sys import argv
import os
from datetime import datetime
from meteor import Meteor, run


class Task(Meteor):
    """
    Export hourly station data
    """

    MODE = argv[1] # 'all' or 'recent'
    name = f'export.bulk.hourly.{MODE}'  # Task name
    use_bulk = True  # Connect to Meteostat Bulk server?
    # dev_mode = True # Run task in dev mode?

    STATIONS_PER_CYCLE = 8 if MODE == 'recent' else 1

    def _write_dump(self, data: list, station: str, year: int = None) -> None:
        """
        Convert DataFrame to CSV and export to bulk server
        """
        # The file path
        path = '/hourly'

        # Filter rows by year if set
        if year is not None:
            path += f'/{year}'
            data = list(
                filter(lambda row: int(row[0].year) == year, data)
            )

        # Export data dump
        self.export_csv(
            list(map(
                lambda d: d[:13],
                data
            )),
            f'{path}/{station}.csv.gz'
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
                        ) if flag is not None else None for flag in d[13:]
                    ]
                ),
                data
            )),
            f'{path}/{station}.map.csv.gz'
        )

    def main(self) -> None:
        """
        Main script & entry point
        """
        stations = self.get_stations('''
            SELECT
                `stations`.`id` AS `id`
            FROM `stations`
            WHERE
                `stations`.`id` IN (
                    SELECT DISTINCT `station`
                    FROM `inventory`
                    WHERE
                        `mode` IN ('H', 'P')
                )
        ''', self.STATIONS_PER_CYCLE)

        # Start & end year
        now = datetime.now()
        start_year = now.year - 1 if self.MODE == 'recent' else 1890
        end_year = now.year + 1

        # Export data for each weather station
        for station in stations:
            with open(
                f'{os.path.dirname(os.path.realpath(__file__))}/hourly.sql',
                'r',
                encoding='UTF-8'
            ) as sql:
                result = self.query(sql.read(), {
                    'station': station[0],
                    'start_datetime': f'{start_year}-01-01 00:00:00',
                    'end_datetime': f'{end_year}-12-31 23:59:59'
                })
                sql.close()

            if result.rowcount > 0:
                # Fetch data
                data = result.fetchall()

                # Write all data
                if self.MODE == 'all':
                    self._write_dump(data, station[0])

                # Write annually
                first_year = int(data[0][0].year)
                last_year = int(data[-1][0].year)

                for year in range(first_year, last_year + 1):
                    self._write_dump(data, station[0], year)


# Run task
run(Task)
