"""
Export SYNOP data

The code is licensed under the MIT license.
"""

from jasper import Jasper
from jasper.helpers import read_file, get_stations
from jasper.actions import export_csv


# General configuration
STATIONS_PER_CYCLE = 10

# Create Jasper instance
jsp = Jasper("export.bulk.raw.synop")

# Get weather station(s)
stations = get_stations(jsp, read_file("synop_stations.sql"), STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:
    try:
        result = jsp.query(
            read_file("synop.sql"), {"station": station[0]}
        )

        if result.rowcount > 0:
            # Fetch data
            data = result.fetchall()

            # Write annually
            first_year = int(data[0][0])
            last_year = int(data[-1][0])

            for year in range(first_year, last_year + 1):
                try:
                    d = list(filter(lambda row: int(row[0]) == year, data))

                    if len(d) > 0:
                        # Export data dump
                        export_csv(
                            jsp, [list(result.keys())] + d, f"/raw/synop/{year}/{station[0]}.csv.gz"
                        )
                except:
                    pass
    except:
        pass

# Close Jasper instance
jsp.close()
