"""
Jasper Core Class

The code is licensed under the MIT license.
"""

from typing import Union
import os
from ftplib import FTP
from configparser import ConfigParser
from sqlalchemy import create_engine, text


class Jasper:
    """
    Core class which is used to talk to internal interfaces like
    the Meteostat DB and Meteostat Bulk
    """

    # Name of the instance
    name: str = ""

    # Running in dev environment?
    dev_mode = False

    # Path of configuration file
    _config_path: str = (
        os.path.expanduser("~") + os.sep + ".meteor" + os.sep + "config.ini"
    )

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
            "mysql+mysqlconnector://"
            + self.config.get("sys_db", "user")
            + ":"
            + self.config.get("sys_db", "password")
            + "@"
            + self.config.get("sys_db", "host")
            + "/"
            + self.config.get("sys_db", "name")
            + "?charset=utf8"
        )

    def _connect_db(self) -> None:
        """
        Connect to Meteostat DB
        """
        self.db = create_engine(
            "mysql+mysqlconnector://"
            + self.config.get("db", "user")
            + ":"
            + self.config.get("db", "password")
            + "@"
            + self.config.get("db", "host")
            + "/"
            + self.config.get("db", "name")
            + "?charset=utf8"
        )

    def _connect_bulk(self) -> None:
        """
        Connect to Meteostat Bulk server
        """
        self.bulk = FTP(self.config.get("bulk", "host"))

        self.bulk.login(
            self.config.get("bulk", "user"), self.config.get("bulk", "password")
        )

    def __init__(
        self, name: str, dev: bool = False, db: bool = True, bulk: bool = False
    ) -> None:
        """
        Initialize Jasper
        """
        # Save name & dev mode flag
        self.name = name
        self.dev_mode = dev

        # Configuration file
        self.config = ConfigParser()
        self.config.read(self._config_path)

        # Connect to system DB
        self._connect_sys()

        # Meteostat DB connection
        if db:
            self._connect_db()

        # Bulk FTP connection
        if bulk:
            self._connect_bulk()

    def set_var(self, name: str, value: str) -> None:
        """
        Set a variable (scoped by task name)
        """
        if self.dev_mode:
            return None

        payload = {"ctx": self.name, "name": name, "value": str(value)}

        with self._sys_db.connect() as con:
            con.execute(
                text(
                    """
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
                """
                ),
                payload,
            )

    def get_var(self, name: str, default=None, type_ref=str) -> str:
        """
        Retrieve a variable (scoped by task name)
        """
        payload = {"ctx": self.name, "name": name}

        with self._sys_db.connect() as con:
            result = con.execute(
                text(
                    """
                    SELECT
                        `value`
                    FROM
                        `variables`
                    WHERE
                        `ctx` = :ctx AND
                        `name` = :name
                    LIMIT 1
                """
                ),
                payload,
            )

        if result.rowcount == 1:
            return type_ref(result.first()[0])

        return default

    def query(self, query: str, payload: dict = None):
        """
        Execute an SQL query on the Meteostat DB
        """
        with self.db.connect() as con:
            return con.execute(text(query).execution_options(autocommit=True), payload)
