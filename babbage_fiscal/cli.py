from __future__ import print_function
from __future__ import absolute_import

import click

from .loader import FDPLoader
from .model_registry import ModelRegistry
from .config import get_engine


@click.group()
def cli():
    pass


@cli.command(name="load-fdp")
@click.option('--package', metavar='<path>', required=True,
              help='Path/URL to the directory where datapackage.json resides')
def load_fdp(package):
    FDPLoader().load_fdp_to_db(package)


@cli.command(name="create-tables")
def create_tables():
    list(ModelRegistry().list_models())

