from babbage_fiscal.util import model_name
from babbage_fiscal.resources import normalize_resource


def translate_fdp(fdp):
    table_name, columns = normalize_resource(fdp)
    model = {
        'fact_table': table_name,
        'measures': {},
        'dimensions': {}
    }

    mapping = fdp.get('mapping')
    for measure in mapping.get('measures', {}):
        name = model_name(measure['name'])
        model['measures'][name] = {
            'label': measure['name'],
            'column': columns[measure['source']]
        }

    for dimension in mapping.get('dimensions', {}):
        dim_name = model_name(dimension['name'])
        model['dimensions'][dim_name] = {
            'attributes': {}
        }
        for field in dimension['fields']:
            field['column'] = field['source']
            field.pop('source')
            fname = field['name']
            field.pop('name')
            model['dimensions'][dim_name]['attributes'][fname] = field

    return model
