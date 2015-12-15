from __future__ import print_function    # (at top of module)
from __future__ import absolute_import    # (at top of module)

import click

from .app import runserver
from .loader import load_fdp_to_db

@click.group()
def cli():
    pass

@cli.command(name="load-fdp")
@click.option('--package', metavar='<path>', required=True,
              help='Path/URL to the directory where datapackage.json resides')
def load_fdp(package):
    load_fdp_to_db(package)

@cli.command(name="run-server")
@click.option('--port', metavar='<port>', default=5000, type=click.INT,
              help='listening port')
def run_server(port):
    runserver(port)

if __name__ == "__main__":
    cli()
