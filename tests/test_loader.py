import time
import os

from unittest import TestCase

import sqlite3
from elasticsearch import Elasticsearch, NotFoundError

from babbage_fiscal import config, loader, model_registry, tasks
from babbage_fiscal.db_utils import table_name_for_package
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

        conn = sqlite3.connect(self.dbfilename)
        c = conn.cursor()
        tablename = table_name_for_package('example@example.com', 'boost-moldova')
        c.execute("SELECT * FROM %s LIMIT 1" % tablename)
        row = c.fetchone()

        # Test factor
        assert(row[-3] == 49756100000.0)
        assert(row[-2] == 51906100)
        assert(row[-1] == 5171022338)

        # Test id
        # Admin classification 1, 1+2, 3, 3+4
        c = ["Central", "101", "0101", "", "", "010"]
        assert(row[2] == c[0])
        assert(row[3] == ' - '.join([c[0], c[1]]))
        assert(row[5] == c[2])
        assert(row[7] == ' - '.join([c[2], c[3]]))
        assert(row[9] == c[4])
        assert(row[11] == c[5])
        # Func classification 1, 1+2
        c = ["01", "01.01"]
        assert(row[13] == c[0])
        assert(row[15] == ' - '.join([c[0], c[1]]))
        # Econ classification 1, 2
        c = ["111", "111.00"]
        assert(row[17] == c[0])
        assert(row[19] == c[1])

        conn.close()

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
        result_output = result.wait(timeout=30, interval=0.5)
        self.cm = model_registry.ModelRegistry()
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')
