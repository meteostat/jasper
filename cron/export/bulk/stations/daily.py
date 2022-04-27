"""
Export daily bulk data

The code is licensed under the MIT license.
"""

from jasper import Jasper
from jasper.helpers import read_file, get_stations
from jasper.actions import export_csv


# General configuration
STATIONS_PER_CYCLE = 10

# Create Jasper instance
jsp = Jasper("export.bulk.daily")

# Get weather station(s)
stations = get_stations(jsp, read_file("daily_stations.sql"), STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:
    result = jsp.query(
        read_file("daily.sql"), {"station": station[0], "timezone": station[1]}
    )

    if result.rowcount > 0:
        # Fetch data
        data = result.fetchall()

        # Export data dump
        export_csv(
            jsp, list(map(lambda d: d[:11], data)), f"/daily/{station[0]}.csv.gz"
        )

        # Export source map
        # pylint: disable=consider-using-generator
        export_csv(
            jsp,
            list(
                map(
                    lambda d: d[:1]
                    + tuple(
                        [
                            "".join(sorted(list(set(flag))))
                            if flag is not None
                            else None
                            for flag in d[11:]
                        ]
                    ),
                    data,
                )
            ),
            f"/daily/{station[0]}.map.csv.gz",
        )

# Close Jasper instance
jsp.close()
