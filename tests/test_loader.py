from babbage_fiscal import config, loader

def setup():
    config._set_connection_string('sqlite:///:memory:')

def test_correct_file_load_success():
    loader.load_fdp('https://raw.githubusercontent.com/akariv/boost-peru-national/master/datapackage.json')
    # loader.load_fdp('data-sample/datapackage.json')
