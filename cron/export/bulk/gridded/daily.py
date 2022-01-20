"""
Export daily grid data

The code is licensed under the MIT license.
"""

import os
import sys
from pathlib import Path
import datetime
import numpy as np
import pandas as pd
import pyproj
import verde as vd
from meteor import Meteor, run


class Task(Meteor):
    """
    Export daily grid data
    """

    MODE = sys.argv[1]  # 'historical' or 'recent'
    name = f'export.bulk.gridded.daily.{MODE}'  # Task name
    use_bulk = True  # Connect to Bulk server?
    # dev_mode = True  # Running in dev mode?

    PARAMETERS = [
        'tavg',
        'tmin',
        'tmax',
        'prcp',
        'wspd',
        'pres'
    ]
    modified: bool = None
    raw: pd.DataFrame

    def _get_delta(self) -> int:
        """
        Get time delta
        """
        if self.MODE == 'historical':
            # We'll need to run at least twice a day
            # Otherwise this is stuck
            delta = self.get_var('date_offset', 0, int)
            if delta > 18493:
                self.set_var('date_offset', 0)
                sys.exit()
            self.set_var('date_offset', delta + 1)
        else:
            delta = int(sys.argv[2])

        return delta

    def _get_modified(self, file: str) -> None:
        """
        Get a files modification date
        """
        try:
            self.modified = self.bulk.voidcmd(file)[4:].strip()
        except BaseException:
            pass

        if self.modified is not None:
            self.modified = datetime.datetime.strptime(
                self.modified,
                '%Y%m%d%H%M%S'
            )
            age = datetime.datetime.now() - self.modified
            if age.days < 3:
                sys.exit()

    def main(self) -> None:
        """
        Main script & entry point
        """
        delta = self._get_delta()

        # Get date
        date = datetime.date.today() - datetime.timedelta(
            days=delta
        )

        # Get modification time
        self._get_modified(f'''MDTM /gridded/daily/tavg/{
            date.strftime("%Y-%m-%d")
        }.nc''')

        # Export data for all weather stations
        raw: pd.DataFrame = pd.read_sql(f'''
            SET STATEMENT
                max_statement_time=3000
            FOR
            SELECT
                ROUND(`stations`.`latitude`, 2) AS `latitude`,
                ROUND(`stations`.`longitude`, 2) AS `longitude`,
                SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority`), ",", 1) AS `tavg`,
                SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority`), ",", 1) AS `tmin`,
                SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority`), ",", 1) AS `tmax`,
                SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority`), ",", 1) AS `prcp`,
                SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ",", 1) AS `wspd`,
                SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ",", 1) AS `pres`
            FROM (
                (SELECT
                    `station`,
                    `tavg`,
                    `tmin`,
                    `tmax`,
                    `prcp`,
                    `wspd`,
                    `pres`,
                    "A" AS `priority`
                FROM `daily_national`
                WHERE
                    `date` = "{date.strftime('%Y-%m-%d')}"
                )
            UNION ALL
                (SELECT
                    `station`,
                    `tavg`,
                    `tmin`,
                    `tmax`,
                    `prcp`,
                    `wspd`,
                    NULL AS `pres`,
                    "A" AS `priority`
                FROM `daily_ghcn`
                WHERE
                    `date` = "{date.strftime('%Y-%m-%d')}"
                )
            UNION ALL
                (SELECT
                    `hourly_model`.`station` AS `station`,
                    IF(count(`hourly_model`.`temp`)<24, NULL, ROUND(AVG(`hourly_model`.`temp`),1)) AS `tavg`,
                    IF(count(`hourly_model`.`temp`)<24, NULL, MIN(`hourly_model`.`temp`)) AS `tmin`,
                    IF(count(`hourly_model`.`temp`)<24, NULL, MAX(`hourly_model`.`temp`)) AS `tmax`,
                    IF(count(`hourly_model`.`prcp`)<24, NULL, SUM(`hourly_model`.`prcp`)) AS `prcp`,
                    IF(count(`hourly_model`.`wspd`)<24, NULL, ROUND(AVG(`hourly_model`.`wspd`),1)) AS `wspd`,
                    IF(count(`hourly_model`.`pres`)<24, NULL, ROUND(AVG(`hourly_model`.`pres`),1)) AS `pres`,
                    "E" AS `priority`
                FROM `hourly_model`
                FORCE INDEX (TIME)
                INNER JOIN `stations`
                ON
                    `hourly_model`.`station` = `stations`.`id` AND
                    `hourly_model`.`time` BETWEEN
                        DATE_SUB("{date.strftime('%Y-%m-%d')} 00:00:00", INTERVAL 12 HOUR) AND
                        DATE_ADD("{date.strftime('%Y-%m-%d')} 23:59:59", INTERVAL 12 HOUR)
                WHERE
                    `hourly_model`.`time` BETWEEN
                        CONVERT_TZ("{date.strftime('%Y-%m-%d')} 00:00:00", `stations`.`tz`, "UTC") AND
                        CONVERT_TZ("{date.strftime('%Y-%m-%d')} 23:59:59", `stations`.`tz`, "UTC")
                GROUP BY
                    `station`
                )
            ) AS `daily_derived`
            INNER JOIN `stations`
            ON
                `daily_derived`.`station` = `stations`.`id`
            WHERE
                `tavg` IS NOT NULL
                OR `tmin` IS NOT NULL
                OR `tmax` IS NOT NULL
                OR `prcp` IS NOT NULL
            GROUP BY
                `station`
        ''', self.db)

        # Clean DataFrame
        # pylint: disable=no-member
        raw = raw.drop(raw[raw.latitude < -89.9].index)
        raw = raw.drop(raw[raw.latitude > 89.9].index)
        raw = raw.drop(raw[raw.longitude < -179.9].index)
        raw = raw.drop(raw[raw.longitude > 179.9].index)

        # Process all parameters
        if len(raw.index):

            for parameter in self.PARAMETERS:

                # Create subset
                df = raw[['latitude', 'longitude', parameter]]

                # Remove NaN values
                df = df[df[parameter].notna()]

                if len(df.index) > 250:
                    # Convert to float
                    df = df.astype(np.float64)

                    # Use Mercator projection because Spline is a Cartesian
                    # gridder
                    projection = pyproj.Proj(
                        proj="merc", lat_ts=df.latitude.mean())
                    proj_coords = projection(
                        df.longitude.values, df.latitude.values)
                    region = vd.get_region((df.longitude, df.latitude))

                    # The desired grid spacing in degrees
                    # (converted to meters using 1 degree approx. 111km)
                    spacing = 1

                    # Loop over the combinations and collect
                    # the scores for each parameter set
                    spline = vd.Spline(mindist=5e3, damping=1e-4)
                    spline.fit(proj_coords, df[parameter])

                    # Cross-validated gridder
                    grid = spline.grid(
                        region=region,
                        spacing=spacing,
                        projection=projection,
                        dims=["lat", "lon"],
                        data_names="value"
                    )

                    # Mask grid points that are too far from the given data
                    # points
                    mask = vd.distance_mask(
                        (df.longitude, df.latitude),
                        maxdist=spacing * 111e3,
                        coordinates=vd.grid_coordinates(
                            region, spacing=spacing),
                        projection=projection
                    )

                    # Export grid as NetCDF4
                    grid = grid.where(mask)
                    filename = f'{Path(__file__).parent}/temp.nc'
                    grid.to_netcdf(filename)

                    # Transfer to bulk server
                    with open(filename, 'rb') as file:
                        self.bulk.cwd(f'/gridded/daily/{parameter}')
                        self.bulk.storbinary(
                            f'STOR {date.strftime("%Y-%m-%d")}.nc',
                            file
                        )

                    # Remove temp file
                    if os.path.exists(filename):
                        os.remove(filename)


# Run task
run(Task)
