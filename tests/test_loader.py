from unittest import TestCase

from babbage_fiscal import config, loader, SQLCubeManager


class CliTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        config._set_connection_string('sqlite:///:memory:')
        self.cm = SQLCubeManager.SQLCubeManager(config.get_engine())

    def test_correct_file_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        loader.load_fdp_to_db('https://raw.githubusercontent.com/akariv/boost-peru-national/master/datapackage.json')
        # loader.load_fdp('data-sample/datapackage.json')
        self.assertGreater(len(list(self.cm.list_cubes())), 0, 'no dataset was loaded')
