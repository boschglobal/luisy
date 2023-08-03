import tempfile
import unittest
from unittest.mock import patch

from luisy import Config
from luisy.cli import build
from luisy.tasks.base import DatabricksTask
from luisy.decorators import deltatable_input
from luisy.decorators import deltatable_output
from luisy.testing import create_testing_config


@deltatable_input(catalog='A', schema='B', table_name='raw')
@deltatable_output(catalog='A', schema='B', table_name='interim')
class ToySparkTask(DatabricksTask):

    def run(self):

        df = self.input().read()
        self.write(df)


class TestSparkTask(unittest.TestCase):

    def setUp(self):

        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):

        # Detour hash_computation
        compute_hashes.return_value = {
            "/A.B.interim.DeltaTable": "2"
        }

        task = ToySparkTask()

        Config().spark.data['A.B.raw'] = {'a': 1, 'b': 2}
        # Make sure that table does not exist before run
        self.assertNotIn('A.B.interim', Config().spark.tables)

        build(task=task, download=False)

        # Make sure that table is written
        self.assertIn('A.B.interim', Config().spark.tables)
