from SQLCubeManager import SQLCubeManager
from config import get_engine
from datapackage import DataPackage
from jtssql import SchemaTable

from .fdp_utils import fdp_to_model
from .db_utils import database_name

def tranlator_iterator(iter,translations):
    for rec in iter:
        yield dict((translations[k],v) for k,v in rec.items())

def load_fdp_to_db(package):

    # Load and validate the datapackage
    dpo = DataPackage(package, schema='fiscal')
    dpo.validate()
    resource = dpo.resources[0]
    schema = resource.metadata['schema']

    # Use the cube manager to get the table name
    engine = get_engine()
    cube_manager = SQLCubeManager(engine)
    datapackage_name = dpo.metadata['name']
    table_name = cube_manager.table_name_for_package( datapackage_name )

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
    table.load_iter(tranlator_iterator(resource.data, field_translation))

    # Create Babbage Model
    model = fdp_to_model(dpo, table_name, resource, field_translation)
    cube_manager.save_model(datapackage_name, package, model)

