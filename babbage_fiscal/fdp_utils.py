def fdp_to_model(package, table_name, resource, field_translator):
    """
    Create a Babbage Model from a Fiscal DataPackage descriptor
    :param package: datapackage object
    :param table_name: db table name to use
    :param resource: resource to load (in the datapackage object)
    :param field_translator: dict for translating resource attribute names to valid db column names
    :return: Babbage Model
    """
    model = {
        'fact_table': table_name,
        'measures': {},
        'dimensions': {}
    }

    mapping = package.metadata['mapping']
    resource_name = resource.metadata['name']

    # Converting measures
    for name,measure in mapping['measures'].items():
        if resource_name != measure.get('resource',resource_name):
            continue
        babbage_measure = {
            'label':name,
            'column':field_translator[measure['source']],
        }
        model['measures'][name]=babbage_measure

    # Converting dimensions
    for name,dimension in mapping['dimensions'].items():
        attributes = dimension['attributes']
        primaryKeys = dimension['primaryKey']
        if not isinstance(primaryKeys,list):
            primaryKeys = [primaryKeys]
        # Marking which attributes have labels
        labels = {}
        for label_name, attr in attributes.items():
            if attr.has_key('labelfor'):
                labels[attr['labelfor']] = label_name
        # Flattening multi-key dimensions into separate dimensions
        for attr in primaryKeys:
            if len(primaryKeys) > 1:
                label = name + '.' + attr
            else:
                label = name
            source = field_translator[attributes[attr]['source']]
            babbage_dimension = {
                'attributes': {
                    source: {'column':source,'label':attr}
                },
                'label': label,
                'key_attribute': source,
                'group': name,
            }
            if labels.has_key(attr):
                label = labels[attr]
                label_source = field_translator[attributes[label]['source']]
                babbage_dimension['attributes'][label_source] = {'column':label_source,'label':label}
                babbage_dimension['label_attribute'] = label_source
            model['dimensions'][source] = babbage_dimension

    return model
