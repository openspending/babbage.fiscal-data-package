import os

from sqlalchemy import create_engine

connection_string = os.environ.get('FISCAL_PACKAGE_ENGINE',u'postgresql://osuser:1234@localhost/os')

engine = create_engine(connection_string)
