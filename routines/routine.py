"""
Routine Class

The code is licensed under the MIT license.
"""

import os
from sqlalchemy import create_engine, text
from configparser import ConfigParser
import pandas as pd


class Routine():

    """
    Generic logic which is used across different kinds of routines.
    """

    # Name of the routine
    name: str = None

    # Path of configuration file
    config_path: str = os.path.expanduser(
        '~') + os.sep + '.routines' + os.sep + 'config.txt'

    # System database connection
    sysdb = None

    # Meteostat database connection
    db = None

    def _connect(self) -> None:

        # Configuration file
        config = ConfigParser()
        config.read(self.config_path)

        # System database connection
        sysdb = create_engine(
            f"""mysql+mysqlconnector://{config.get('sysdb', 'user')}:{config.get('sysdb', 'password')}@{config.get('sysdb', 'host')}/{config.get('sysdb', 'name')}""")
        self.sysdb = sysdb

        # Meteostat database connection
        msdb = create_engine(
            f"""mysql+mysqlconnector://{config.get('msdb', 'user')}:{config.get('msdb', 'password')}@{config.get('msdb', 'host')}/{config.get('msdb', 'name')}""")
        self.db = msdb

    def __init__(
        self,
        name: str
    ) -> None:

        # Meta data
        self.name = name

        # Database connections
        self._connect()

    def set_var(self, name: str, value: str) -> None:

        payload = {
            'ctx': self.name,
            'name': name,
            'value': str(value)
        }

        with self.sysdb.connect() as con:
            con.execute(
                text("""INSERT INTO `variables`(`ctx`, `name`, `value`) VALUES (:ctx, :name, :value) ON DUPLICATE KEY UPDATE `value` = :value"""),
                payload)

    def get_var(self, name: str) -> str:

        payload = {
            'ctx': self.name,
            'name': name
        }

        with self.sysdb.connect() as con:
            result = con.execute(
                text(
                    """SELECT `value` FROM `variables` WHERE `ctx` = :ctx AND `name` = :name LIMIT 1"""),
                payload)

        if result.rowcount == 1:
            return result.first()[0]
        else:
            return None

    def get_stations(self, query: str, limit: int) -> list:

        # Get counter value
        counter = self.get_var('station_counter')
        skip = 0 if counter is None else int(counter)

        # Get weather stations
        with self.db.connect() as con:
            result = con.execute(text(query + f" LIMIT {skip}, {limit}"))

        # Update counter
        if result.rowcount < limit:
            self.set_var('station_counter', 0)
        else:
            self.set_var('station_counter', skip + limit)

        return result.fetchall()

    def write(self, data: pd.DataFrame, schema: dict) -> None:

        # Validations
        for parameter, validation in schema['validation'].items():
            if parameter in data.columns:
                data[parameter] = data[parameter].apply(validation)

        # NaN to None
        data = data.where(pd.notnull(data), None)

        # Remove rows with NaN only
        data = data.dropna(axis=0, how='all')

        # Convert time data to String
        data.index = data.index.set_levels(
            data.index.levels[1].astype(str), level=1)

        with self.db.begin() as con:
            for record in data.reset_index().to_dict(orient='records'):
                con.execute(text(schema['import_query']), {
                            **schema['template'], **record})

    def read(self, query: str) -> None:

        return pd.read_sql(query, self.db)
