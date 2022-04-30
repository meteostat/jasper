"""
Export hourly bulk data

The code is licensed under the MIT license.
"""

from sys import argv
from datetime import datetime
from jasper import Jasper
from jasper.helpers import read_file, get_stations
from jasper.actions import export_csv


# Task mode
# 'all' or 'recent'
MODE = argv[1]

# General configuration
STATIONS_PER_CYCLE = 16 if MODE == "recent" else 1

# Create Jasper instance
jsp = Jasper(f"export.bulk.hourly.{MODE}")


def write_dump(data: list, station: str, year: int = None) -> None:
    """
    Convert DataFrame to CSV and export to bulk server
    """
    # The file path
    path = "/hourly"

    # Filter rows by year if set
    if year is not None:
        path += f"/{year}"
        data = list(filter(lambda row: int(row[0].year) == year, data))

    # Export data dump
    export_csv(jsp, list(map(lambda d: d[:13], data)), f"{path}/{station}.csv.gz")

    # Export source map
    # pylint: disable=consider-using-generator
    export_csv(
        jsp,
        list(
            map(
                lambda d: d[:2]
                + tuple(
                    [
                        "".join(sorted(list(set(flag)))) if flag is not None else None
                        for flag in d[13:]
                    ]
                ),
                data,
            )
        ),
        f"{path}/{station}.map.csv.gz",
    )


# Get weather stations
stations = get_stations(
    jsp,
    read_file("hourly_stations.sql" if MODE == "all" else "hourly_stations_recent.sql"),
    STATIONS_PER_CYCLE,
)

# Start & end year
now = datetime.now()
start_year = now.year - 1 if MODE == "recent" else 1890
end_year = now.year + 1

# Export data for each weather station
for station in stations:
    result = jsp.query(
        read_file("hourly.sql"),
        {
            "station": station[0],
            "start_datetime": f"{start_year}-01-01 00:00:00",
            "end_datetime": f"{end_year}-12-31 23:59:59",
        },
    )

    if result.rowcount > 0:
        # Fetch data
        data = result.fetchall()

        # Write all data
        if MODE == "all":
            write_dump(data, station[0])

        # Write annually
        first_year = int(data[0][0].year)
        last_year = int(data[-1][0].year)

        for year in range(first_year, last_year + 1):
            write_dump(data, station[0], year)

# Close Jasper instance
jsp.close()
