from __future__ import print_function    # (at top of module)

import click

from .app import runserver
from .loader import load_fdp

@click.group()
def cli():
    pass

@cli.command(name="load-fdp")
@click.option('--package', metavar='<path>', required=True,
              help='Path/URL to the directory where datapackage.json resides')
def load_fdp(package):
    loader.load_fdp(package)

@cli.command(name="run-server")
@click.option('--port', metavar='<port>', default=5000, type=click.INT,
              help='listening port')
def run_server(port):
    runserver(port)

if __name__=="__main__":
    cli()
