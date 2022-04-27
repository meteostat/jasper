"""
Update normals inventory

The code is licensed under the MIT license.
"""

from meteostat import Normals
from jasper import Jasper
from jasper.helpers import get_stations


# General configuration
Normals.max_age = 0
STATIONS_PER_CYCLE = 10

# Create Jasper instance
jsp = Jasper("task.inventory.normals")

stations = get_stations(
    jsp,
    "SELECT `id` FROM `stations`",
    STATIONS_PER_CYCLE,
)

if len(stations) > 0:
    for station in stations:
        try:
            # Get daily data from Meteostat
            data = Normals(station[0], "all").fetch()

            # Get start & end dates of time series
            start = data.index.get_level_values("start").min()
            end = data.index.get_level_values("end").max()
            start = f"{start}-01-01"
            end = f"{end}-12-31"

            if len(start) == 10 and len(end) == 10:
                jsp.query(
                    f"""
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
                """
                )

        except BaseException:
            pass

# Close Jasper instance
jsp.close()
