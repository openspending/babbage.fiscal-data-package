import os
from os_package_registry import PackageRegistry


class ModelRegistry(PackageRegistry):

    def __init__(self, es=None):
        if es is not None:
            super(ModelRegistry, self).__init__(es_instance=es)
        else:
            super(ModelRegistry, self).__init__(
                es_connection_string=os.environ.get('OS_ELASTICSEARCH_ADDRESS','localhost:9200'))


