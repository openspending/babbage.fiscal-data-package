from concurrent.futures import ThreadPoolExecutor
import requests

from model_registry import ModelRegistry
from config import get_engine
from datapackage import DataPackage
from jtssql import SchemaTable

from .fdp_utils import fdp_to_model
from .db_utils import database_name


def _translator_iterator(iter,translations):
    for rec in iter:
        yield dict((translations[k],v) for k,v in rec.items())


class FDPLoader(object):
    """
    Utility class for loading FDPs to the DB
    """

    def __init__(self, engine=None):
        if engine is None:
            self.engine = get_engine()
        else:
            self.engine = engine
        self.executor = ThreadPoolExecutor(max_workers=1)

    @staticmethod
    def load_fdp_to_db(package, engine=None):
        """
        Load an FDP to the database, create a babbage model and save it as well
        :param package: URL for the datapackage.json
        """

        # Load and validate the datapackage
        if engine is None:
            engine = get_engine()
        dpo = DataPackage(package, schema='fiscal')
        dpo.validate()
        resource = dpo.resources[0]
        schema = resource.metadata['schema']

        # Use the cube manager to get the table name
        registry = ModelRegistry(engine)
        datapackage_name = dpo.metadata['name']
        table_name = registry.table_name_for_package( datapackage_name )

        all_fields = set()
        field_translation = {}
        # Process schema - slugify field names
        for field in schema['fields']:
            name = database_name(field['name'], all_fields)
            all_fields.add(name)
            field_translation[field['name']] = name
            field['name'] = name

        # Load 1st resource data into DB
        table = SchemaTable(engine, table_name, schema)
        if table.exists:
            table.drop()
        table.create()
        table.load_iter(_translator_iterator(resource.data, field_translation))

        # Create Babbage Model
        model = fdp_to_model(dpo, table_name, resource, field_translation)
        registry.save_model(datapackage_name, package, model)

    @staticmethod
    def _load_fdp_with_callback(package, callback, engine):
        """
        Load an FDP to the DB and call a callback when done
        :param package: URL for the datapackage.json file
        :param callback: URL to call when the import is successful
        """
        FDPLoader.load_fdp_to_db(package, engine)
        requests.get(callback)

    def start_loading_in_bg(self, package, callback):
        """
        Load an FDP to the DB in the background
        :param package: URL for the datapackage.json file
        :param callback: URL to call when the import is successful
        """
        self.executor.submit(FDPLoader._load_fdp_with_callback, package, callback, self.engine)

    def _shutdown(self):
        """
        Shutdown the executor, waiting for all tasks to complete
        :return: nothing
        """
        self.executor.shutdown(wait=True)
