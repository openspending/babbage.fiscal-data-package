import os

from sqlalchemy import create_engine

_connection_string = os.environ.get('FISCAL_PACKAGE_ENGINE')#,u'postgresql://osuser:1234@localhost/os')


def _set_connection_string(connection_string):
    global _connection_string
    _connection_string = connection_string


def get_connection_string():
    return _connection_string


def get_engine():
    """Return engine singleton"""
    return create_engine(_connection_string)

