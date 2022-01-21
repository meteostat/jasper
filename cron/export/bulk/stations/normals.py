"""
Export normals bulk data

The code is licensed under the MIT license.
"""

from datetime import datetime
from io import BytesIO, StringIO
from gzip import GzipFile
import numpy as np
import pandas as pd
from meteostat import Monthly
from meteor import Meteor, run


class Task(Meteor):
    """
    Export climate normals
    """

    name = 'export.bulk.normals'  # Task name
    use_bulk = True  # Connect to Meteostat Bulk server?
    # dev_mode = True  # Run task in dev mode?

    Monthly.max_age = 0
    STATIONS_PER_CYCLE = 10

    @staticmethod
    def _get_bulk(station: list) -> pd.DataFrame:
        """
        Get climate normals from Meteostat Bulk interface
        """
        try:
            # Full DataFrame
            data: pd.DataFrame = pd.DataFrame()

            # Get decades
            decades = []
            loop_year = 1990
            current_year = datetime.now().year
            while loop_year < current_year:
                decades.append(loop_year)
                loop_year += 10

            # Collect reference period normals from Monthly interface
            for year in decades:
                try:
                    # Start & end year
                    start = datetime(year - 29, 1, 1)
                    end = datetime(year, 12, 31)
                    # Get data
                    df = Monthly(station[0], start, end)
                    # Get coverage data
                    coverage = {}
                    # pylint: disable=protected-access
                    for parameter in df._columns[2:]:
                        coverage[parameter] = df.coverage(parameter)
                    # Fetch DataFrame
                    df = df.fetch()
                    # Drop certain columns
                    df = df.drop(['tavg', 'snow', 'wdir', 'wpgt'], axis=1)
                    # Aggregate monthly
                    df = df.groupby(df.index.month).agg('mean')
                    df = df.round(1)
                    # Refactor index
                    df.reset_index(inplace=True)
                    df = df.reset_index()
                    df['start'] = year - 29
                    df['end'] = year
                    df.set_index(['start', 'end', 'time'], inplace=True)
                    # Remove uncertain data
                    # pylint: disable=consider-using-dict-items
                    for parameter in coverage:
                        if parameter in df.columns and coverage[parameter] < (
                                1 / 3):
                            df[parameter] = np.NaN

                    # Add to full DataFrame
                    data = data.append(df)

                except BaseException:
                    pass

            # Return full DataFrame
            return data

        except BaseException:
            return pd.DataFrame()

    def _get_database(self, station: list) -> pd.DataFrame:
        """
        Get climate normals from Meteostat DB
        """
        try:
            # Get data from DB
            df = pd.read_sql(
                f'''
                    SET STATEMENT
                        max_statement_time=60
                    FOR
                    SELECT
                        `start`,
                        `end`,
                        `month` AS `time`,
                        `tmin`,
                        `tmax`,
                        `prcp`,
                        `pres`,
                        `tsun`
                    FROM
                        `normals_global`
                    WHERE
                        `station` = "{station[0]}"
                    ORDER BY
                        `start`,
                        `end`,
                        `month`
                ''',
                self.db,
                index_col=['start', 'end', 'time']
            )

            # Return DataFrame
            return df

        except BaseException:
            return pd.DataFrame()

    def main(self) -> None:
        """
        Main script & entry point
        """
        # Get weather station(s)
        stations = self.get_stations('''
            SELECT
                `id`,
                `latitude`,
                `longitude`,
                `altitude`
            FROM
                `stations`
            WHERE
                `id` IN (
                    SELECT DISTINCT `station`
                    FROM `inventory`
                ) OR
                `id` IN (
                    SELECT DISTINCT `station`
                    FROM `normals_global`
                )
        ''', self.STATIONS_PER_CYCLE)

        # Export data for each weather station
        for station in stations:
            try:
                # Get data
                bulk = Task._get_bulk(station)
                database = self._get_database(station)

                # Merge data
                if bulk.index.size > 0 or database.index.size > 0:
                    data = pd.concat([bulk, database])
                    data = data.groupby([
                        data.index.get_level_values('start'),
                        data.index.get_level_values('end'),
                        data.index.get_level_values('time')]
                    ).agg('last')
                else:
                    continue

                # Drop needless column(s)
                try:
                    data = data.drop('index', axis=1)
                except BaseException:
                    pass

                # Drop NaN-only rows
                data = data.dropna(how='all')
                data = data.sort_index()

                if data.index.size > 0:
                    file = BytesIO()

                    with GzipFile(fileobj=file, mode='w') as gz:
                        output = StringIO()
                        data.to_csv(output, header=False)
                        gz.write(output.getvalue().encode())
                        gz.close()
                        file.seek(0)

                    self.cd_tree('/normals')
                    self.bulk.storbinary(
                        f'STOR /normals/{station[0]}.csv.gz', file)

            except BaseException:
                pass


# Run task
run(Task)
