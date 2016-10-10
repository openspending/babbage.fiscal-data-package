import logging
import traceback

from celery import Celery

from .callbacks import STATUS_LOADING_DATA
from .config import get_engine, _set_connection_string
from .loader import FDPLoader
from .callbacks import do_request, STATUS_INITIALIZING, STATUS_DONE, STATUS_FAIL

app = Celery('fdp_loader')
app.config_from_object('babbage_fiscal.celeryconfig')

logging.root.setLevel(logging.DEBUG)


class ProgressSender(object):

    def __init__(self, callback, package):
        self.count = 0
        self.callback = callback
        self.package = package

    def __call__(self, status=STATUS_LOADING_DATA, count=None, data=None, error=None):
        if count is None:
            count = self.count
        else:
            self.count = count
        do_request(self.callback, self.package, status,
                   progress=count, error=error, data=data)


@app.task
def load_fdp_task(package, callback, connection_string=None):
    send_progress = ProgressSender(callback, package)
    if connection_string is not None:
        _set_connection_string(connection_string)
    try:
        logging.info("Starting to load %s" % package)
        send_progress(status=STATUS_INITIALIZING)
        assert FDPLoader.load_fdp_to_db(package, get_engine(), send_progress)
        logging.info("Finished to load %s" % package)
    except Exception as e:
        exc = traceback.format_exc()
        send_progress(status=STATUS_FAIL, error=str(exc))
        print("Failed to load %s: %s" % (package, exc))

