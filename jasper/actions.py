"""
Jasper Export Tools

The code is licensed under the MIT license.
"""

import csv
from gzip import GzipFile
from io import BytesIO, StringIO
import json
import os
from sqlalchemy import text
import pandas as pd
from .core import Jasper
from .helpers import bulk_cd


def export_csv(jsp: Jasper, data: list, filename: str) -> None:
    """
    Store data in CSV format on Meteostat Bulk
    """
    # Print to console and abort if in dev mode
    if jsp.dev_mode:
        print(data)
        return None

    # Create a file
    file = BytesIO()

    # Write gzipped CSV data
    if len(data) > 0:
        with GzipFile(fileobj=file, mode="w") as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=",")
            writer.writerows(data)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

    # Change into directory
    bulk_cd(jsp, os.path.dirname(os.path.abspath(filename)))

    # Store file
    jsp.bulk.storbinary(f"STOR {filename}", file)


def export_json(jsp: Jasper, data: list, filename: str) -> None:
    """
    Store data in JSON format on Meteostat Bulk
    """
    # Print to console and abort if in dev mode
    if jsp.dev_mode:
        print(data)
        return None

    # Create a file
    file = BytesIO()

    # Write gzipped JSON data
    if len(data) > 0:
        with GzipFile(fileobj=file, mode="w") as gz:
            gz.write(
                json.dumps(data, indent=4, default=str, ensure_ascii=False).encode()
            )
            gz.close()
            file.seek(0)

    # Change into directory
    bulk_cd(jsp, os.path.dirname(os.path.abspath(filename)))

    # Store file
    jsp.bulk.storbinary(f"STOR {filename}", file)


def persist(jsp: Jasper, data: pd.DataFrame, schema: dict) -> None:
    """
    Import a Pandas DataFrame into the Meteostat DB
    """
    # Validations
    for parameter, validation in schema["validation"].items():
        if parameter in data.columns:
            data[parameter] = data[parameter].apply(validation)

    # NaN to None
    data = data.where(pd.notnull(data), None)

    # Remove rows with NaN only
    data = data.dropna(axis=0, how="all")

    # Convert time data to String
    data.index = data.index.set_levels(data.index.levels[1].astype(str), level=1)

    # Print to console and abort if in dev mode
    if jsp.dev_mode:
        print(data)
        return None

    with jsp.db.begin() as con:
        for record in data.reset_index().to_dict(orient="records"):
            con.execute(text(schema["import_query"]), {**schema["template"], **record})
