import tempfile
from unittest.mock import patch

import os



from luisy import Config
from luisy.cli import build
from luisy.tasks.base import DatabricksTask
from luisy.decorators import deltatable_input
from luisy.decorators import deltatable_output
from luisy.testing import (
    create_testing_config,
    LuisyTestCase,
    debug_run,
)


@deltatable_input(table_name='def')
@deltatable_output(table_name='abc')
class ToySparkTask(DatabricksTask):

    def run(self):
        self.write(None)


class TestSparkTask(LuisyTestCase):

    def setUp(self):

        Config().set_param("databricks_host", "some_cluster_http")
        Config().set_param('databricks_cluster_id', "some_cluster_id")

        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):
        compute_hashes.return_value = {
            "/catalog.schema.abc.ABC": "123"
        }
        task = ToySparkTask()
        coordinator = build(task=task, download=False)
