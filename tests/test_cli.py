import os

from unittest import TestCase

from sqlalchemy.engine.reflection import Inspector

from babbage_fiscal.cli import cli
from babbage_fiscal import model_registry, config

from click.testing import CliRunner

from .test_common import SAMPLE_PACKAGES

MODEL_NAME, SAMPLE_PACKAGE = SAMPLE_PACKAGES['md']

class LoaderTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the test
        :return:
        """
        self.runner = CliRunner()
        config._set_connection_string('sqlite:///test.db')

    def tearDown(self):
        os.unlink('test.db')

    def test_load_fdp_cmd_success(self):
        """
        Simple invocation of the load-fdp command
        """
        # loader.load_fdp('https://raw.githubusercontent.com/akariv/boost-peru-national/master/datapackage.json')
        result = self.runner.invoke(cli,
                           args=['load-fdp', '--package', SAMPLE_PACKAGE],
                           env={'FISCAL_PACKAGE_ENGINE':'sqlite:///test.db'})
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_create_tables_cmd_success(self):
        """
        Simple invocation of the create-tables command
        """
        result = self.runner.invoke(cli,
                           args=['create-tables'],
                           env={'FISCAL_PACKAGE_ENGINE':'sqlite:///test.db'})
        engine = config.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertTrue('models' in inspector.get_table_names())
