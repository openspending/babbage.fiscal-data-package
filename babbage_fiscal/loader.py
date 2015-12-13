from SQLCubeManager import SQLCubeManager
from config import get_engine
from datapackage import DataPackage
from jtssql import SchemaTable

from .fdp_utils import fdp_to_model

def load_fdp(package):

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

    # Load 1st resource data into DB
    table = SchemaTable(engine, table_name, schema)
    if table.exists:
        table.drop()
    table.create()
    table.load_iter(resource.data)

    # Create Babbage Model
    model = fdp_to_model(dpo,table_name,resource)
    cube_manager.save_model( datapackage_name, package, model )
