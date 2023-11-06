"""
Export METAR data

The code is licensed under the MIT license.
"""

from jasper import Jasper
from jasper.helpers import read_file, get_stations
from jasper.actions import export_csv


# General configuration
STATIONS_PER_CYCLE = 12

# Create Jasper instance
jsp = Jasper("export.bulk.raw.metar")

# Get weather station(s)
stations = get_stations(jsp, read_file("metar_stations.sql"), STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:
    result = jsp.query(
        read_file("metar.sql"), {"station": station[0]}
    )

    if result.rowcount > 0:
        # Fetch data
        data = result.fetchall()

        # Export data dump
        export_csv(
            jsp, list(map(lambda d: d[:11], data)), f"/raw/metar/{station[0]}.csv.gz"
        )

# Close Jasper instance
jsp.close()
