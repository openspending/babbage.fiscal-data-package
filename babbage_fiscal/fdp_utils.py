def fdp_to_model(package, table_name, resource):
    '''Create a Babbage Model from a Fiscal DataPackage descriptor'''
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
            #'description':,
            'column':measure['source'],
        }
        model['measures'][name]=babbage_measure

    # Converting dimensions
    for name,dimension in mapping['dimensions'].items():
        attributes = {}
        for attr in dimension['attributes']:
            attributes.update(attr)
        primaryKeys = dimension['primaryKey']
        prefix = ''
        if not isinstance(primaryKeys,list):
            primaryKeys = [primaryKeys]
        # Flattening multi-key dimensions into separate dimensions
        for attr in primaryKeys:
            if len(primaryKeys)>1:
                label = name + '.' + attr
            else:
                label = name
            source = attributes[attr]['source']
            babbage_dimension = {
                'attributes': {
                    source: {'column':source,'label':attr}
                },
                'label':label,
                'key_attribute': source
            }
            model['dimensions'][source] = babbage_dimension

    return model