"""
Export meta data for weather stations

The code is licensed under the MIT license.
"""

import json
from jasper import Jasper
from jasper.helpers import read_file
from jasper.actions import export_csv, export_json


# Create Jasper instance
jsp = Jasper("export.bulk.stations.meta", bulk=True)

# Export data for all weather stations
result = jsp.query(read_file("meta.sql"))

if result.rowcount > 0:
    # Fetch data
    data = result.fetchall()

    # Data lists
    full = []
    lite = []
    slim = []

    for record in data:
        # Create dict of names
        try:
            names = json.loads(record[2])
        except BaseException:
            names = {}
        names["en"] = record[1]

        # Create station object
        station_object = {
            "id": record[0],
            "name": names,
            "country": record[3],
            "region": record[4],
            "identifiers": {"national": record[5], "wmo": record[6], "icao": record[7]},
            "location": {
                "latitude": record[8],
                "longitude": record[9],
                "elevation": record[10],
            },
            "timezone": record[11],
            "inventory": {
                "model": {"start": record[13], "end": record[14]},
                "hourly": {"start": record[15], "end": record[16]},
                "daily": {"start": record[17], "end": record[18]},
                "monthly": {"start": record[19], "end": record[20]},
                "normals": {"start": record[21], "end": record[22]},
            },
        }

        # Add to full dump
        full.append(station_object)

        # Check if any data is available
        if (
            record[14] is not None
            or record[16] is not None
            or record[18] is not None
            or record[20] is not None
            or record[22] is not None
        ):
            lite.append(station_object)
            # Convert to list
            record = record.values()
            # Add slim rows
            slim_cols = [0, 1, 3, 4, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20]
            slim.append([record[i] for i in slim_cols])

    # Write JSON dumps
    export_json(jsp, full, "/stations/full.json.gz")
    export_json(jsp, lite, "/stations/lite.json.gz")

    # Write CSV dumps
    export_csv(jsp, slim, "/stations/slim.csv.gz")
    export_csv(jsp, [row[0:13] for row in slim], "/stations/lib.csv.gz")

# Close Jasper instance
jsp.close()