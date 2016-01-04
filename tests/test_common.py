import os

# SAMPLE_PACKAGE = 'https://raw.githubusercontent.com/openspending/fiscal-data-package-demos/update-to-reflect-new-specs/boost-moldova/datapackage.json'
# SAMPLE_PACKAGE = '/Users/adam/code/os/bbdfp/boost/fiscal-data-package-demos/boost-moldova/datapackage.json'
SAMPLE_PACKAGES = {
    'md': (u'boost-moldova', os.path.abspath('tests/sample-data/md/datapackage.json')),
    'uk': (u'ukgov-finances-cra', os.path.abspath('tests/sample-data/uk/datapackage.json')),
}
NUM_RECORDS = 2000
