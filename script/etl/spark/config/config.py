"""
config.py
~~~~~~~~
Module containing configuration for the application
"""

from os import environ
import json


class Config(object):
    def __init__(self):
        self._mongodb_host = (
            environ["MONGODB_HOST"] if "MONGODB_HOST" in environ else "localhost"
        )

        self._mongodb_port = (
            environ["MONGODB_PORT"] if "MONGODB_PORT" in environ else "27017"
        )

        self._mongodb_password = (
            environ["MONGODB_PASSWORD"] if "MONGODB_PASSWORD" in environ else "toor"
        )

        self._mongodb_user = (
            environ["MONGODB_USER"] if "MONGODB_USER" in environ else "root"
        )
        

    def get_mongodb_host(self):
        return self._mongodb_host

    def get_mongodb_port(self):
        return self._mongodb_port

    def get_mongodb_password(self):
        return self._mongodb_password

    def get_mongodb_user(self):
        return self._mongodb_user

    def get_config_from_json(self, file_name):
        with open(file_name) as json_file:
            data = json.load(json_file)
        return data
