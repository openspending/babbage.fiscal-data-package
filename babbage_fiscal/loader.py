import time
import email.utils

from datapackage import DataPackage
from jsontableschema_sql import Storage

from babbage_fiscal.callbacks import *
from .model_registry import ModelRegistry
from .config import get_engine
from .fdp_utils import fdp_to_model
from .db_utils import database_name, table_name_for_package


def factorize(factors, rec, key):
    if key in factors:
        return rec[key]*factors[key]
    return rec[key]


def _translator_iterator(it, field_order, factors, callback):
    count = 0
    for rec in it:
        count += 1
        if count % 1000 == 1 and callback is not None:
            callback(count=count)
        rec = (count,) + tuple(factorize(factors, rec, k) for k in field_order)
        yield rec


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
    def load_fdp_to_db(package, engine=None, callback=None):
        """
        Load an FDP to the database, create a babbage model and save it as well
        :param package: URL for the datapackage.json
        :param engine: DB engine
        :param callback: callback to use to send progress updates
        """

        # Load and validate the datapackage
        if engine is None:
            engine = get_engine()
        if callback is None:
            callback = noop
        callback(status=STATUS_LOADING_DATAPACKAGE)
        dpo = DataPackage(package, schema='fiscal')
        callback(status=STATUS_VALIDATING_DATAPACKAGE)
        dpo.validate()
        callback(status=STATUS_LOADING_RESOURCE)
        resource = dpo.resources[0]
        schema = resource.descriptor['schema']

        # Use the cube manager to get the table name
        registry = ModelRegistry()
        datapackage_name = dpo.descriptor['name']
        datapackage_owner = dpo.descriptor['owner']
        datapackage_author = dpo.descriptor['author']

        # Get the full name from the author field, and rewrite it without the email
        fullname, email_addr = email.utils.parseaddr(datapackage_author)
        email_addr = email_addr.split('@')[0] + '@not.shown'
        dpo.descriptor['author'] = '{0} <{1}>'.format(fullname, email_addr)
        dpo.descriptor.setdefault('private', True)

        # Measure factors
        measures = dpo.descriptor.get('model',{}).get('measures',{})
        factors = {}
        for _, measure in measures.items():
            factor = measure.get('factor',1)
            if factor != 1:
                factors[measure.get('source')] = factor

        model_name = "{0}:{1}".format(datapackage_owner, datapackage_name)
        table_name = table_name_for_package(datapackage_owner, datapackage_name)

        all_fields = set()
        field_translation = {}
        field_order = []
        # Process schema - slugify field names
        for field in schema['fields']:
            name = database_name(field['name'], all_fields)
            all_fields.add(name)
            translated_field = {
                'name': name,
                'type': field['type']
            }
            field_translation[field['name']] = translated_field
            field_order.append(field['name'])

        storage_schema = {
            'fields': [
                {
                    'type': f['type'],
                    'name': field_translation[f['name']]['name'],
                    'format': f.get('format', 'default')
                }
                for f in schema['fields']
                ],
            # Babbage likes just one primary key
            'primaryKey': '_id'
        }

        # Add Primary key to schema
        storage_schema['fields'].insert(0, {
            'name': '_id',
            'type': 'integer'
        })

        # Create Babbage Model
        callback(status=STATUS_CREATING_BABBAGE_MODEL)
        model = fdp_to_model(dpo, table_name, resource, field_translation)

        # Create indexes
        indexes = []
        primary_keys = resource.descriptor['schema'].get('primaryKey',[])
        for dim in model['dimensions'].values():
            if dim['label'] in primary_keys:
                key_field = dim['attributes'][dim['key_attribute']]['label']
                key_field = field_translation[key_field]['name']
                indexes.append((key_field,))

                label_field = dim['attributes'].get(dim.get('label_attribute'), {}).get('label')
                if label_field is not None:
                    label_field = field_translation[label_field]['name']
                    if label_field != key_field:
                        indexes.append((key_field, label_field))


        # Load 1st resource data into DB
        storage = Storage(engine)
        if storage.check(table_name):
            callback(status=STATUS_DELETING_TABLE)
            storage.delete(table_name)
        callback(status=STATUS_CREATING_TABLE)
        storage.create(table_name, storage_schema, indexes)

        callback(status=STATUS_LOADING_DATA_READY)
        storage.write(table_name, _translator_iterator(resource.iter(), field_order, factors, callback))

        callback(status=STATUS_SAVING_METADATA)
        registry.save_model(model_name, package, dpo.descriptor,
                            model, datapackage_name, fullname)
        return model_name, dpo.descriptor, model
