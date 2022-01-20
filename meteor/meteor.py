"""
Meteor Class

The code is licensed under the MIT license.
"""

import os
from io import BytesIO, StringIO
from gzip import GzipFile
import csv
import json
from ftplib import FTP
from configparser import ConfigParser
from sqlalchemy import create_engine, text
import pandas as pd


class Meteor():
    """
    Core class which is used to talk to internal interfaces like
    the Meteostat DB and Meteostat Bulk
    """

    # Name of the instance
    name: str = ''

    # Use flags
    use_db: bool = True
    use_bulk: bool = False

    # Running in dev environment?
    dev_mode = False

    # Path of configuration file
    _config_path: str = os.path.expanduser(
        '~') + os.sep + '.meteor' + os.sep + 'config.ini'

    # The config
    config = None

    # System database connection
    _sys_db = None

    # Meteostat database connection
    db = None

    # Bulk FTP connection
    bulk = None

    def _connect_sys(self) -> None:
        """
        Connect to Meteostat System DB
        """
        self._sys_db = create_engine(
            "mysql+mysqlconnector://" +
            self.config.get('sys_db', 'user') +
            ":" +
            self.config.get('sys_db', 'password') +
            "@" +
            self.config.get('sys_db', 'host') +
            "/" +
            self.config.get('sys_db', 'name') +
            "?charset=utf8"
        )

    def _connect_db(self) -> None:
        """
        Connect to Meteostat DB
        """
        self.db = create_engine(
            'mysql+mysqlconnector://' +
            self.config.get('db', 'user') +
            ':' +
            self.config.get('db', 'password') +
            '@' +
            self.config.get('db', 'host') +
            '/' +
            self.config.get('db', 'name') +
            '?charset=utf8'
        )

    def _connect_bulk(self) -> None:
        """
        Connect to Meteostat Bulk server
        """
        self.bulk = FTP(
            self.config.get('bulk', 'host')
        )

        self.bulk.login(
            self.config.get('bulk', 'user'),
            self.config.get('bulk', 'password')
        )

    def setup(self) -> None:
        # Configuration file
        self.config = ConfigParser()
        self.config.read(self._config_path)

        # Connect to system DB
        self._connect_sys()

        # Meteostat DB connection
        if self.use_db:
            self._connect_db()

        # Bulk FTP connection
        if self.use_bulk:
            self._connect_bulk()

    def set_var(self, name: str, value: str) -> None:
        """
        Set a variable (scoped by task name)
        """
        if self.dev_mode:
            return None

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

    def get_var(self, name: str, default=None, type=str) -> str:
        """
        Retrieve a variable (scoped by task name)
        """
        payload = {
            'ctx': self.name,
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
            return type(result.first()[0])

        return default

    def get_stations(self, query: str, limit: int) -> list:
        """
        Get list of weather stations based on counter
        """
        # Get counter value
        skip = self.get_var('station_counter', 0, int)

        # Get weather stations
        with self.db.connect() as con:
            result = con.execute(text(query + f" LIMIT {skip}, {limit}"))

        # Update counter
        if result.rowcount < limit:
            self.set_var('station_counter', 0)
        else:
            self.set_var('station_counter', skip + limit)

        return result.fetchall()

    def persist(self, data: pd.DataFrame, schema: dict) -> None:
        """
        Import a Pandas DataFrame into the Meteostat DB
        """
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

        # Print to console and abort if in dev mode
        if self.dev_mode:
            print(data)
            return None

        with self.db.begin() as con:
            for record in data.reset_index().to_dict(orient='records'):
                con.execute(text(schema['import_query']), {
                            **schema['template'], **record})

    def cd_tree(self, path: str) -> None:
        """
        Change into directory path and create missing directories
        """
        # Go to root directory
        self.bulk.cwd('/')

        # Create directory tree
        directories = list(filter(None, path.split('/')))

        # Process directory tree
        for directory in directories:
            try:
                self.bulk.cwd(str(directory))
            except:
                self.bulk.mkd(str(directory))
                self.bulk.cwd(str(directory))            

    def export_csv(self, data: list, filename: str) -> None:
        """
        Store data in CSV format on Meteostat Bulk
        """
        # Print to console and abort if in dev mode
        if self.dev_mode:
            print(data)
            return None

        # Create a file
        file = BytesIO()

        # Write gzipped CSV data
        if len(data) > 0:
            with GzipFile(fileobj=file, mode='w') as gz:
                output = StringIO()
                writer = csv.writer(output, delimiter=',')
                writer.writerows(data)
                gz.write(output.getvalue().encode())
                gz.close()
                file.seek(0)

        # Change into directory
        self.cd_tree(
            os.path.dirname(os.path.abspath(filename))
        )

        # Store file
        self.bulk.storbinary(f'STOR {filename}', file)

    def export_json(self, data: list, filename: str) -> None:
        """
        Store data in JSON format on Meteostat Bulk
        """
        # Print to console and abort if in dev mode
        if self.dev_mode:
            print(data)
            return None

        # Create a file
        file = BytesIO()

        # Write gzipped JSON data
        if len(data) > 0:
            with GzipFile(fileobj=file, mode='w') as gz:
                gz.write(
                    json.dumps(
                        data,
                        indent=4,
                        default=str,
                        ensure_ascii=False
                    ).encode()
                )
                gz.close()
                file.seek(0)

        # Change into directory
        self.cd_tree(
            os.path.dirname(os.path.abspath(filename))
        )

        # Store file
        self.bulk.storbinary(f'STOR {filename}', file)

    def query(self, query: str, payload: dict = None):
        """
        Execute an SQL query on the Meteostat DB
        """
        with self.db.connect() as con:
            return con.execute(
                text(query).execution_options(autocommit=True),
                payload
            )
