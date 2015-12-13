from unittest import TestCase

from babbage import BabbageException
from babbage_fiscal import config, loader, SQLCubeManager


class CubeManagerTest(TestCase):

    MODEL_NAME = u'peru-boost-nacional-2012-2014-truncated'

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        config._set_connection_string('sqlite:///:memory:')
        loader.load_fdp_to_db('https://raw.githubusercontent.com/akariv/boost-peru-national/master/datapackage.json')

    def test_list_cubes_correct_values(self):
        """
        Simple loading of one valid fdp into DB and testing correct CM values
        """
        cm = SQLCubeManager.SQLCubeManager(config.get_engine())
        models = list(cm.list_cubes())
        self.assertEquals(len(models), 1, 'no dataset was loaded')
        self.assertEquals(models[0], self.MODEL_NAME, 'dataset with wrong name')

    def test_get_cube_model_correct_values(self):
        cm = SQLCubeManager.SQLCubeManager(config.get_engine())
        model = cm.get_cube_model(self.MODEL_NAME)
        self.assertEquals(model['fact_table'],'fdp_peru_boost_nacional_2012_2014_truncated','bad model')

    def test_get_cube_correct_values(self):
        cm = SQLCubeManager.SQLCubeManager(config.get_engine())
        cube = cm.get_cube(self.MODEL_NAME)
        facts = cube.facts(page_size=5)
        self.assertEquals(facts['total_fact_count'],19999,'wrong number of records')

    def test_has_cube_correct_values(self):
        cm = SQLCubeManager.SQLCubeManager(config.get_engine())
        self.assertTrue(cm.has_cube(u'peru-boost-nacional-2012-2014-truncated'))
        self.assertFalse(cm.has_cube(u'peru-boost-nacional-2012-2014-truncated1'))

    def test_no_such_cube(self):
        cm = SQLCubeManager.SQLCubeManager(config.get_engine())
        self.assertRaises(BabbageException, cm.get_cube_model, 'bla')