"""
Importer Class

The code is licensed under the MIT license.
"""

from routines import Routine


class Importer(Routine):

    """
    Import data into the Meteostat database
    """

    # Name of the import routine
    name: str = None

    # Data type
    type: str = None

    def __init__(
        self,
        name: str,
        type: str
    ) -> None:

        # Meta data
        self.name = name
        self.type = type

        # Database connection
        self._connect()
