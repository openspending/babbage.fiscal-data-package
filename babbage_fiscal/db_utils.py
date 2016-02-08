# coding: utf-8
from normality import slugify

TABLE_NAME_PREFIX = "fdp_"


def model_name(name):
    """ Generate a normalized version of a model name. """
    column = slugify(name or '', sep='_')
    return TABLE_NAME_PREFIX + column.strip('_')


def database_name(name, names=[], default='column'):
    """ Generate a normalized version of the column name. """
    column = slugify(name or '', sep='_')
    column = column.strip('_')
    column = default if not len(column) else column
    name, i = column, 2
    _name = name
    # de-dupe: column, column_2, column_3, ...
    while name in names:
        name = '%s_%s' % (_name, i)
        i += 1
    return name
