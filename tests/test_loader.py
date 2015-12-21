import time
import os

from unittest import TestCase

from babbage_fiscal import config, loader, model_registry, tasks
from test_common import SAMPLE_PACKAGE


class LoaderTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        self.dbfilename = os.path.join(os.path.abspath('.'), 'test.db')
        self.connection_string = 'sqlite:///' + self.dbfilename
        config._set_connection_string(self.connection_string)
        self.loader = loader.FDPLoader()

    def tearDown(self):
        os.unlink(self.dbfilename)

    def test_correct_file_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_correct_file_double_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.assertEquals(len(list(self.cm.list_models())), 1, 'no dataset was loaded')

    def test_correct_file_load_supplied_engine_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE, config.get_engine())
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_correct_bg_file_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        result = tasks.load_fdp_task.apply_async(args=(SAMPLE_PACKAGE, "http://google.com", self.connection_string))
        result_output = result.wait(timeout=10, interval=0.5)
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')
