import os

from unittest import TestCase

from elasticsearch import Elasticsearch, NotFoundError
from sqlalchemy.engine.reflection import Inspector

from babbage_fiscal.cli import cli
from babbage_fiscal import model_registry, config

from click.testing import CliRunner

from .test_common import SAMPLE_PACKAGES, LOCAL_ELASTICSEARCH

MODEL_NAME, SAMPLE_PACKAGE = SAMPLE_PACKAGES['md']


class CLITest(TestCase):

    def setUp(self):
        """
        Set-up a dummy DB for the test
        :return:
        """
        self.es = Elasticsearch(hosts=[LOCAL_ELASTICSEARCH])
        try:
            self.es.indices.delete(index='packages')
        except NotFoundError:
            pass
        self.runner = CliRunner()
        self.dbfilename = 'test.db'
        config._set_connection_string('sqlite:///test.db')

    def tearDown(self):
        if os.path.exists(self.dbfilename):
            os.unlink(self.dbfilename)

    def test_load_fdp_cmd_success(self):
        """
        Simple invocation of the load-fdp command
        """
        ret = self.runner.invoke(cli,
                           args=['load-fdp', '--package', SAMPLE_PACKAGE],
                           env={'OS_ELASTICSEARCH_ADDRESS': LOCAL_ELASTICSEARCH})
        self.cm = model_registry.ModelRegistry()
        self.assertGreater(len(list(self.cm.list_models())), 0, 'no dataset was loaded')

    def test_create_tables_cmd_success(self):
        """
        Simple invocation of the create-tables command
        """
        self.runner.invoke(cli,
                           args=['create-tables'],
                           env={'OS_ELASTICSEARCH_ADDRESS': LOCAL_ELASTICSEARCH})
        engine = config.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertTrue('models' not in inspector.get_table_names())
