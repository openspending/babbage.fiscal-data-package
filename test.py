from babbage_fiscal import cli, config
config._set_connection_string('sqlite:///:memory:')
cli()

