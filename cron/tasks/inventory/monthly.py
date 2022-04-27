"""
Update monthly inventory

The code is licensed under the MIT license.
"""

from meteostat import Monthly
from jasper import Jasper
from jasper.helpers import get_stations


# General configuration
Monthly.max_age = 0
STATIONS_PER_CYCLE = 10

# Create Jasper instance
jsp = Jasper("task.inventory.monthly")

stations = get_stations(
    jsp,
    "SELECT `id` FROM `stations`",
    STATIONS_PER_CYCLE,
)

if len(stations) > 0:
    for station in stations:
        try:
            # Get monthly data from Meteostat
            data = Monthly(station[0], model=False).fetch()

            # Get start & end dates of time series
            start = data.index.min().strftime("%Y-%m-%d")
            end = data.index.max().strftime("%Y-%m-%d")

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
                        "M",
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