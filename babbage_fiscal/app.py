from flask import Flask

from babbage.api import configure_api

from SQLCubeManager import SQLCubeManager

from config import engine


def runserver(port=5000):
    app = Flask('babbage_fiscal')
    manager = SQLCubeManager(engine)
    blueprint = configure_api(app, manager)
    app.register_blueprint(blueprint, url_prefix='/api')

    app.run(port=port)

if __name__ == "__main__":
    runserver()
