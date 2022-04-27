"""
Jasper Helpers

The code is licensed under the MIT license.
"""

from ftplib import FTP
import os, sys
from sqlalchemy import text
from .core import Jasper


def read_file(path: str, relative=True) -> None:
    """
    Read a file's content
    """
    if relative:
        path = os.path.abspath(os.path.dirname(sys.argv[0])) + "/" + path

    with open(path, "r", encoding="UTF-8") as sql:
        return sql.read()


def bulk_cd(bulk: FTP, path: str) -> None:
    """
    Change into directory path and create missing directories
    """
    # Go to root directory
    bulk.cwd("/")

    # Create directory tree
    directories = list(filter(None, path.split("/")))

    # Process directory tree
    for directory in directories:
        try:
            bulk.cwd(str(directory))
        except BaseException:
            bulk.mkd(str(directory))
            bulk.cwd(str(directory))


def get_stations(jsp: Jasper, query: str, limit: int) -> list:
    """
    Get list of weather stations based on counter
    """
    # Get counter value
    skip = jsp.get_var("station_counter", 0, int)

    # Get weather stations
    with jsp.db().connect() as con:
        result = con.execute(text(query + f" LIMIT {skip}, {limit}"))

    # Update counter
    if result.rowcount < limit:
        jsp.set_var("station_counter", 0)
    else:
        jsp.set_var("station_counter", skip + limit)

    return result.fetchall()
