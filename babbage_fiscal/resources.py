from .db_utils import database_name


def reflect_mapping_resources(mapping):
    """ Find which resources are mentioned in the mapping. """
    resources = set([])
    for measure in mapping.get("measures", {}):
        if 'resource' in measure:
            resources.add(measure['resource'])
    for dimension in mapping.get("dimensions", {}):
        for field in dimension["fields"]:
            if 'resource' in field:
                resources.add(field['resource'])
    return resources


def get_mapping_resource(fdp):
    """ Get the resource to which the FDP mapping refers. """
    names = reflect_mapping_resources(fdp.get('mapping'))
    if len(names) > 1:
        raise ValueError("Mappings which use multiple resources "
                         "are currently unsupported. Please use "
                         "only one resource per FDP.")
    resources = fdp.get('resources', [])
    if len(names) == 0:
        if len(resources) == 1:
            return resources[0]
        raise ValueError("The mapping does not refer to any "
                         "resource. Please specify the resource "
                         "to be used.")
    for resource in resources:
        if names[0] == resource.get('name'):
            return resource
    raise ValueError("The resource specified for the mapping "
                     "could not be found.")


def normalize_resource(fdp):
    """ Convert the column names and table name of a TDP to
    database-safe forms. """
    resource = get_mapping_resource(fdp)
    table = database_name(resource.get('name'))
    columns = {}
    for field in resource.get('schema', {}).get('fields'):
        name = field.get('name')
        columns[name] = database_name(name, columns.values())
    return table, columns


