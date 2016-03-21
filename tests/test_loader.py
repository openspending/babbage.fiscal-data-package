import time
import os

from unittest import TestCase

from elasticsearch import Elasticsearch, NotFoundError

from babbage_fiscal import config, loader, model_registry, tasks
from .test_common import SAMPLE_PACKAGES, LOCAL_ELASTICSEARCH


class LoaderTest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the tests
        :return:
        """
        self.es = Elasticsearch(hosts=[LOCAL_ELASTICSEARCH])
        try:
            self.es.indices.delete(index='packages')
        except NotFoundError:
            pass
        self.dbfilename = os.path.join(os.path.abspath('.'), 'test.db')
        self.connection_string = 'sqlite:///' + self.dbfilename
        config._set_connection_string(self.connection_string)
        self.loader = loader.FDPLoader()

    def tearDown(self):
        if os.path.exists(self.dbfilename):
            os.unlink(self.dbfilename)

    def test_correct_file_load_success(self):
        """
        Simple loading of valid fdp's into DB
        """
        for MODEL_NAME, SAMPLE_PACKAGE in SAMPLE_PACKAGES.values():
            self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
            self.cm = model_registry.ModelRegistry()
        self.assertGreater(len(list(self.cm.list_models())), 1, 'no dataset was loaded')

    def test_correct_file_double_load_success(self):
        """
        Double loading of one valid fdp into DB
        """
        MODEL_NAME, SAMPLE_PACKAGE = SAMPLE_PACKAGES['md']
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE)
        self.cm = model_registry.ModelRegistry()
        self.assertEquals(len(list(self.cm.list_models())), 1, 'no dataset was loaded')

    def test_correct_file_load_supplied_engine_success(self):
        """
        Simple loading of one valid fdp into DB with supplied engine
        """
        MODEL_NAME, SAMPLE_PACKAGE = SAMPLE_PACKAGES['md']
        self.loader.load_fdp_to_db(SAMPLE_PACKAGE, config.get_engine())
        self.cm = model_registry.ModelRegistry()
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_correct_bg_file_load_success(self):
        """
        Simple loading of one valid fdp into DB in background
        """
        MODEL_NAME, SAMPLE_PACKAGE = SAMPLE_PACKAGES['uk']
        result = tasks.load_fdp_task.apply_async(args=(SAMPLE_PACKAGE, "http://google.com", self.connection_string))
        result_output = result.wait(timeout=10, interval=0.5)
        self.cm = model_registry.ModelRegistry()
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')
