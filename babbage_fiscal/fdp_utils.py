from .db_utils import database_name


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

    mapping = package.descriptor['model']
    schema = resource.descriptor['schema']['fields']
    field_titles = dict((f.get('name'), f.get('title', f.get('name'))) for f in schema)
    resource_name = resource.descriptor['name']

    # Converting measures
    all_concepts = set()
    for orig_name, measure in mapping['measures'].items():
        if resource_name != measure.get('resource', resource_name):
            continue
        name = database_name(orig_name, all_concepts, 'measure')
        all_concepts.add(name)
        babbage_measure = {
            'label': field_titles.get(measure['source'], measure['source']),
            'column': field_translator[measure['source']]['name'],
            'orig_measure': orig_name
        }
        if 'currency' in measure:
            babbage_measure['currency'] = measure['currency']
        model['measures'][name]=babbage_measure

    hierarchies = {}

    # Converting dimensions
    for orig_name, dimension in mapping['dimensions'].items():
        # Normalize the dimension name
        name = database_name(orig_name, all_concepts, 'dimension')
        all_concepts.add(name)

        attribute_names = {}
        attributes = dimension['attributes']
        for orig_attr_name in attributes.keys():
            attr_name = database_name(orig_attr_name, attribute_names.values(), 'attr')
            attribute_names[orig_attr_name] = attr_name

        primaryKeys = dimension['primaryKey']
        if not isinstance(primaryKeys,list):
            primaryKeys = [primaryKeys]
        # Marking which attributes have labels
        labels = {}
        for label_name, attr in attributes.items():
            if 'labelfor' in attr:
                labels[attr['labelfor']] = label_name
        # Flattening multi-key dimensions into separate dimensions
        for pkey in primaryKeys:
            # Get slugified name
            translated_pkey = attribute_names[pkey]
            # Get name for the dimension (depending on the number of primary keys)
            if len(primaryKeys) > 1:
                dimname = database_name(orig_name + '_' + translated_pkey, all_concepts, 'dimension')
            else:
                dimname = database_name(orig_name, all_concepts, 'dimension')
            label = field_titles[attributes[pkey]['source']]
            all_concepts.add(dimname)
            # Create dimension and key attribute
            translated_field = field_translator[attributes[pkey]['source']]
            source = translated_field['name']
            type = translated_field['type']
            babbage_dimension = {
                'attributes': {
                    translated_pkey:
                        {'column': source,
                         'label': field_titles[attributes[pkey]['source']],
                         'datatype': type,
                         'orig_attribute': pkey}
                },
                'label': label,
                'key_attribute': translated_pkey,
                'orig_dimension': orig_name
            }
            # Update hierarchies
            hierarchies.setdefault(name, {'levels': [],
                                          'label': name.replace('_',' ').title()}
                                   )['levels'].append(dimname)
            # Add label attributes (if any)
            if pkey in labels:
                label = labels[pkey]
                translated_label_field = field_translator[attributes[label]['source']]
                label_source = translated_label_field['name']
                label_type = translated_label_field['type']
                babbage_dimension['attributes'][attribute_names[label]] = \
                    {
                        'column': label_source,
                        'label': field_titles[attributes[label]['source']],
                        'datatype': label_type,
                        'orig_attribute': label
                    }
                babbage_dimension['label_attribute'] = attribute_names[label]
            # Copy other attributes as well (if there's just one primary key attribute)
            if len(primaryKeys) == 1:
                for attr_name, attr in attributes.items():
                    if attr_name not in (pkey, labels.get(pkey)):
                        translated_attr_field = field_translator[attributes[attr_name]['source']]
                        attr_source = translated_attr_field['name']
                        attr_type = translated_attr_field['type']
                        babbage_dimension['attributes'][attribute_names[attr_name]] = \
                            {
                                'column': attr_source,
                                'label': field_titles[attributes[attr_name]['source']],
                                'datatype': attr_type,
                                'orig_attribute': attr_name
                            }
            model['dimensions'][dimname] = babbage_dimension
        model['hierarchies'] = dict((k,v) for k,v in hierarchies.items())

    return model
