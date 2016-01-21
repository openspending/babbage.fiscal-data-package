import time

from datapackage import DataPackage
from jtssql import SchemaTable

from .model_registry import ModelRegistry
from .config import get_engine
from .fdp_utils import fdp_to_model
from .db_utils import database_name


def _translator_iterator(it, translations, callback):
    count = 0
    for rec in it:
        count += 1
        if count % 1000 == 1 and callback is not None:
            callback(count=count)
        yield dict((translations[k]['name'], v) for k, v in zip(rec.headers, rec.values))


def noop(*args, **kw):
    pass

class FDPLoader(object):
    """
    Utility class for loading FDPs to the DB
    """

    def __init__(self, engine=None):
        if engine is None:
            self.engine = get_engine()
        else:
            self.engine = engine

    @staticmethod
    def load_fdp_to_db(package, engine = None, callback=None):
        """
        Load an FDP to the database, create a babbage model and save it as well
        :param package: URL for the datapackage.json
        """

        # Load and validate the datapackage
        if engine is None:
            engine = get_engine()
        if callback is None:
            callback = noop
        callback(status='load-datapackage')
        dpo = DataPackage(package, schema='fiscal')
        callback(status='validate-datapackage')
        dpo.validate()
        callback(status='load-resource')
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
            translated_field = {
                'name': name,
                'type': field['type']
            }
            field_translation[field['name']] = translated_field
            field['name'] = name

        # Load 1st resource data into DB
        callback(status='table-create')
        table = SchemaTable(engine, table_name, schema)
        if table.exists:
            table.drop()
        table.create()
        callback(status='table-load')
        table.load_iter(_translator_iterator(resource.data, field_translation, callback))

        # Create Babbage Model
        callback(status='model-create')
        model = fdp_to_model(dpo, table_name, resource, field_translation)
        callback(status='model-save')
        registry.save_model(datapackage_name, package, dpo, model)
