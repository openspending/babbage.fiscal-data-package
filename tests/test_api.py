from threading import Condition, Thread

from http.server import HTTPServer, BaseHTTPRequestHandler

from flask import Flask, url_for
from flask.ext.testing import TestCase as FlaskTestCase

from babbage_fiscal.api import babbage_loader
from test_common import SAMPLE_PACKAGE

cv = Condition()

class MyHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        print("GET",self.path)
        with cv:
            cv.notify()
        raise RuntimeError()


class MyHTTPServer(Thread):

    def run(self):
        self.server = HTTPServer(("localhost",7878), MyHandler)
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()

class TestAPI(FlaskTestCase):

    def create_app(self):
        app = Flask('test')
        app.register_blueprint(babbage_loader, url_prefix='/loader')
        app.config['DEBUG'] = True
        app.config['TESTING'] = True
        app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = True
        return app

    def setUp(self):
        super(TestAPI, self).setUp()

    def test_load_package_success(self):
        res = self.client.get(url_for('babbage_loader.load',package=SAMPLE_PACKAGE, callback='http://localhost:7878/callback'))
        self.assertEquals(res.status_code, 200, "Bad status code %r" % res.status_code)
        th = MyHTTPServer()
        th.start()
        with cv:
            cv.wait()
        th.stop()



