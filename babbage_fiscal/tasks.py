from celery import Celery
import requests

from .config import get_engine, _set_connection_string
from .loader import FDPLoader

app = Celery('fdp_loader')
app.config_from_object('babbage_fiscal.celeryconfig')

@app.task
def load_fdp_task(package, callback, connection_string=None):
    if connection_string is not None:
        _set_connection_string(connection_string)
    FDPLoader.load_fdp_to_db(package, get_engine())
    ret = requests.get(callback)
