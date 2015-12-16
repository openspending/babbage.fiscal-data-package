from unittest import TestCase

from babbage_fiscal import config, loader, model_registry
from test_common import SAMPLE_PACKAGE


class LoaderTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        config._set_connection_string('sqlite:///:memory:')
        self.cm = model_registry.ModelRegistry(config.get_engine())
        self.loader = loader.FDPLoader()

    def test_correct_file_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_correct_bg_file_load_success(self):
        """
        Simple loading of one valid fdp into DB
        """
        self.loader.start_loading_in_bg(SAMPLE_PACKAGE,"http://google.com")
        self.loader._shutdown()
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')
