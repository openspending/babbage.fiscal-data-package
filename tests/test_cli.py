import os

from unittest import TestCase

from babbage_fiscal.cli import cli
from babbage_fiscal import model_registry, config

from click.testing import CliRunner

from test_common import SAMPLE_PACKAGE


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
        print result
        print result.output
        self.cm = model_registry.SQLCubeManager(config.get_engine())
        self.assertGreater(len(list(self.cm.list_cubes())), 0, 'no dataset was loaded')
