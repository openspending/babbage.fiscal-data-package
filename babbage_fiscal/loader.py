import email.utils
import logging
import traceback

from datapackage import DataPackage
from jsontableschema_sql import Storage

from .callbacks import *
from .model_registry import ModelRegistry
from .config import get_engine
from .fdp_utils import fdp_to_model
from .db_utils import database_name, table_name_for_package
from .row_processor import RowProcessor


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
    def load_fdp_to_db(package, engine=None, callback=noop):
        """
        Load an FDP to the database, create a babbage model and save it as well
        :param package: URL for the datapackage.json
        :param engine: DB engine
        :param callback: callback to use to send progress updates
        """
        model = None
        model_name = None
        dpo = None
        datapackage_name = None
        fullname = None
        registry = ModelRegistry()

        def status_update(**kwargs):
            if model_name is not None:
                try:
                    _name, _origin_url, _package, _model, _dataset, \
                        _author, _loading_status, _loaded = \
                            registry.get_raw(model_name)
                except KeyError:
                    _name = model_name
                    _origin_url = ''
                    _package = {}
                    _model = {}
                    _dataset = ''
                    _author = ''
                    _loading_status = None
                    _loaded = False

                if model is not None:
                    _model = model
                if dpo is not None:
                    _package = dpo.descriptor
                if datapackage_name is not None:
                    _dataset = datapackage_name
                if fullname is not None:
                    _author = fullname
                status = kwargs.get('status')
                if status is not None:
                    _loading_status = status
                    _loaded = status == STATUS_DONE
                registry.save_model(_name, _origin_url, _package,
                                    _model, _dataset, _author,
                                    _loading_status, _loaded)
            callback(**kwargs)

        # Load and validate the datapackage
        if engine is None:
            engine = get_engine()
        status_update(status=STATUS_LOADING_DATAPACKAGE)
        dpo = DataPackage(package, schema='fiscal')
        status_update(status=STATUS_VALIDATING_DATAPACKAGE)
        dpo.validate()
        status_update(status=STATUS_LOADING_RESOURCE)
        resource = dpo.resources[0]
        schema = resource.descriptor['schema']

        # Use the cube manager to get the table name
        datapackage_name = dpo.descriptor['name']
        datapackage_owner = dpo.descriptor['owner']
        datapackage_author = dpo.descriptor['author']

        # Get the full name from the author field, and rewrite it without the email
        fullname, email_addr = email.utils.parseaddr(datapackage_author)
        email_addr = email_addr.split('@')[0] + '@not.shown'
        dpo.descriptor['author'] = '{0} <{1}>'.format(fullname, email_addr)
        dpo.descriptor.setdefault('private', True)

        model_name = "{0}:{1}".format(datapackage_owner, datapackage_name)
        table_name = table_name_for_package(datapackage_owner, datapackage_name)

        try:
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
            status_update(status=STATUS_CREATING_BABBAGE_MODEL)
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
            # We use the prefix name so that JTS-SQL doesn't load all table data into memory
            storage = Storage(engine, prefix=table_name)
            faux_table_name = ''
            if storage.check(faux_table_name):
                status_update(status=STATUS_DELETING_TABLE)
                storage.delete(faux_table_name)
            status_update(status=STATUS_CREATING_TABLE)
            storage.create(faux_table_name, storage_schema, indexes)

            status_update(status=STATUS_LOADING_DATA_READY)
            row_processor = RowProcessor(resource.iter(), status_update,
                                         schema, dpo.descriptor)
            storage.write(faux_table_name, row_processor.iter())

            response = {
                'model_name': model_name,
                'babbage_model': model,
                'package': dpo.descriptor
            }
            status_update(status=STATUS_DONE, data=response)

        except Exception as e:
            logging.exception('LOADING FAILED')
            status_update(status=STATUS_FAIL, error=traceback.format_exc())
            return False

        return True
