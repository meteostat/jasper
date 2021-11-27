"""
Meteor Class

The code is licensed under the MIT license.
"""

import os
from sys import argv
from ftplib import FTP
from sqlalchemy import create_engine, text
from configparser import ConfigParser
import pandas as pd


class Meteor():

    """
    Core class which is used to talk to internal interfaces like
    the Meteostat DB and Meteostat Bulk
    """

    # Name of the instance
    _name: str = None

    # Running in dev environment?
    _dev = False

    # Path of configuration file
    _config_path: str = os.path.expanduser(
        '~') + os.sep + '.meteor' + os.sep + 'config.txt'

    # The config
    _config = None

    # System database connection
    _sys_db = None

    # Meteostat database connection
    db = None

    # Bulk FTP connection
    bulk = None

    def _connect_sys(self) -> None:

        # System database engine
        self._sys_db = create_engine(f"""
            mysql+mysqlconnector://
                {self.config.get('sys_db', 'user')}:
                {self.config.get('sys_db', 'password')}@
                {self.config.get('sys_db', 'host')}/
                {self.config.get('sys_db', 'name')}
                ?charset=utf8
        """)

    def _connect_db(self) -> None:

        # Meteostat database engine
        self.db = create_engine(f"""
            mysql+mysqlconnector://
                {self.config.get('db', 'user')}:
                {self.config.get('db', 'password')}@
                {self.config.get('db', 'host')}/
                {self.config.get('db', 'name')}
                ?charset=utf8
        """)

    def _connect_bulk(self) -> None:

        # Connect
        self.bulk = FTP(
            self._config.get('bulk', 'host')
        )

        # Login
        self.bulk.login(
            self._config.get('bulk', 'user'),
            self._config.get('bulk', 'password')
        )

    def __init__(
        self,
        name: str,
        db: bool = True, # Connect to Meteostat DB?
        bulk: bool = False, # Connect to Meteostat Bulk FTP server?
        dev: bool = False # Run in dev mode?
    ) -> None:

        # Meta data
        self._name = name

        # Configuration file
        self._config = ConfigParser()
        self._config.read(self.config_path)

        # Connect to system DB
        self._connect_sys()

        # Meteostat DB connection
        if db:
            self._connect_db()

        # Bulk FTP connection
        if bulk:
            self._connect_bulk()

        # Set dev mode
        self._dev = dev

    def set_var(self, name: str, value: str) -> None:

        payload = {
            'ctx': self.name,
            'name': name,
            'value': str(value)
        }

        with self._sys_db.connect() as con:
            con.execute(
                text("""
                    INSERT INTO `variables` (
                        `ctx`,
                        `name`,
                        `value`
                    )
                    VALUES (
                        :ctx,
                        :name,
                        :value
                    )
                    ON DUPLICATE KEY UPDATE
                        `value` = :value
                """),
                payload
            )

    def get_var(self, name: str, default=None) -> str:

        payload = {
            'ctx': self._name,
            'name': name
        }

        with self._sys_db.connect() as con:
            result = con.execute(
                text("""
                    SELECT
                        `value`
                    FROM
                        `variables`
                    WHERE
                        `ctx` = :ctx AND
                        `name` = :name
                    LIMIT 1
                """),
                payload
            )

        if result.rowcount == 1:
            return result.first()[0]
        else:
            return default

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

    def import(self, data: pd.DataFrame, schema: dict) -> None:

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

    def query(self, query: str, payload: dict = {}):

        with self._sys_db.connect() as con:
            return con.execute(
                text(query).execution_options(autocommit=True),
                payload
            )

    @staticmethod
    def run(ref: class) -> None:

        params = argv
        params.pop()

        ref(*params)