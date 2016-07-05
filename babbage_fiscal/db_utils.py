# coding: utf-8
from slugify import slugify

TABLE_NAME_PREFIX = "fdp__"


def model_name(owner, name):
    """ Generate a normalized version of a model name. """
    name = slugify(name or '', separator='_', max_length=28).strip('_')
    owner = slugify(owner or '', separator='_', max_length=28).strip('_')
    return TABLE_NAME_PREFIX + owner + '__' + name


def table_name_for_package(datapackage_owner, datapackage_name):
    return model_name(datapackage_owner, datapackage_name)


def database_name(name, names=[], default='column'):
    """ Generate a normalized version of the column name. """
    column = slugify(name or '', separator='_', max_length=30)
    column = column.strip('_')
    column = default if not len(column) else column
    name, i = column, 2
    _name = name
    # de-dupe: column, column_2, column_3, ...
    while name in names:
        name = '%s_%s' % (_name, i)
        i += 1
    return name
