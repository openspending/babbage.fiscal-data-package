from __future__ import print_function
from __future__ import absolute_import

import click

from .loader import FDPLoader

@click.group()
def cli():
    pass


@cli.command(name="load-fdp")
@click.option('--package', metavar='<path>', required=True,
              help='Path/URL to the directory where datapackage.json resides')
def load_fdp(package):
    FDPLoader().load_fdp_to_db(package)

