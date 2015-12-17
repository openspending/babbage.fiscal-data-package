from unittest import TestCase

from babbage.cube import Cube
from babbage_fiscal import config, loader, model_registry
from test_common import SAMPLE_PACKAGE, NUM_RECORDS, MODEL_NAME


class RegistryTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        config._set_connection_string('sqlite:///:memory:')
        fdp_loader = loader.FDPLoader(config.get_engine())
        fdp_loader.load_fdp_to_db(SAMPLE_PACKAGE)

    def test_list_cubes_correct_values(self):
        """
        Simple loading of one valid fdp into DB and testing correct CM values
        """
        cm = model_registry.ModelRegistry(config.get_engine())
        models = list(cm.list_models())
        self.assertEquals(len(models), 1, 'no dataset was loaded')
        self.assertEquals(models[0], MODEL_NAME, 'dataset with wrong name')

    def test_get_cube_model_correct_values(self):
        cm = model_registry.ModelRegistry(config.get_engine())
        model = cm.get_model(MODEL_NAME)
        self.assertEquals(model['fact_table'],'fdp_'+MODEL_NAME.replace('-','_'),'bad model')

    def test_get_cube_correct_values(self):
        cm = model_registry.ModelRegistry(config.get_engine())
        model = cm.get_model(MODEL_NAME)
        cube = Cube(config.get_engine(), model['fact_table'], model)
        facts = cube.facts(page_size=5)
        self.assertEquals(facts['total_fact_count'], NUM_RECORDS, 'wrong number of records')

    def test_has_cube_correct_values(self):
        cm = model_registry.ModelRegistry(config.get_engine())
        self.assertTrue(cm.has_model(MODEL_NAME))
        self.assertFalse(cm.has_model(MODEL_NAME+'1'))

    def test_no_such_cube(self):
        cm = model_registry.ModelRegistry(config.get_engine())
        self.assertRaises(KeyError, cm.get_model, 'bla')