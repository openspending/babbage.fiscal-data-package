# coding: utf-8
from normality import slugify


def model_name(name):
    """ Generate a normalized version of a model name. """
    column = slugify(name or '', sep='_')
    return column.strip('_')


def database_name(name, names=[]):
    """ Generate a normalized version of the column name. """
    column = slugify(name or '', sep='_')
    column = column.strip('_')
    column = 'column' if not len(column) else column
    name, i = column, 2
    # de-dupe: column, column_2, column_3, ...
    while name in names:
        name = '%s_%s' % (name, i)
        i += 1
    return name
