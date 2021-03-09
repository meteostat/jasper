"""
Routine Class

The code is licensed under the MIT license.
"""

import os
from sqlalchemy import create_engine
from configparser import ConfigParser


class Routine():

    """
    Generic logic which is used across different kinds of routines.
    """

    # The configuration
    config_path: str = os.path.expanduser(
        '~') + os.sep + '.routines' + os.sep + 'config.txt'

    # The database connection
    db = None

    def _connect(self) -> None:

        # Configuration file
        config = ConfigParser()
        config.read(self.config_path)

        # Database connection
        engine = create_engine(
            f"""mysql+mysqlconnector://{config.get('database', 'user')}:{config.get('database', 'password')}@{config.get('database', 'host')}/{config.get('database', 'name')}""")
        self.db = engine
