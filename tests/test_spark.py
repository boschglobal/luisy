import tempfile
import unittest
import glob
from unittest.mock import patch

import pandas as pd
import numpy as np
import os
import pyspark
import luisy
from luisy import Config
from luisy.cli import build
from luisy.tasks.base import SparkTask
from luisy.decorators import deltatable_input
from luisy.decorators import deltatable_output
from luisy.testing import create_testing_config


@deltatable_input(catalog='A', schema='B', table_name='raw')
@deltatable_output(catalog='A', schema='B', table_name='interim')
class ToySparkTask(SparkTask):

    def run(self):

        df = self.input().read()
        self.write(df)


@luisy.requires(ToySparkTask)
@luisy.auto_filename
@luisy.interim
class LocalTask(SparkTask):

    def run(self):
        df = self.input().read()
        self.write(df)


@luisy.csv_output
@luisy.raw
class LocalFile(luisy.ExternalTask):

    def run(self):
        df = pd.DataFrame(data=np.random.randint(10, size=(100, 10)))
        self.write(df)


@luisy.requires(LocalFile)
@luisy.interim
@luisy.parquetdir_output(outdir='some_dir')
class LocalPyTask(SparkTask):

    def run(self):
        df = self.read_input()
        self.write(df)


class TestSparkTask(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.df_test = pd.DataFrame(data={'a': [1], 'b': [2]})
        Config().spark.data['A.B.raw'] = self.df_test

        self.hashes = {
            "/A.B.interim.DeltaTable": "2",
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    @patch("luisy.hashes.compute_hashes")
    def test_local_task(self, compute_hashes):
        self.hashes.update({"/tests/interim/LocalTask.pkl": "3"})

        # Detour hash_computation
        compute_hashes.return_value = self.hashes

        task = LocalTask()

        build(task=task, download=False)

        self.assertTrue(os.path.exists(task.get_outfile()))
        pd.testing.assert_frame_equal(task.read(), self.df_test)

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):

        # Detour hash_computation
        compute_hashes.return_value = self.hashes

        task = ToySparkTask()

        # Make sure that table does not exist before run
        self.assertNotIn('A.B.interim', Config().spark.tables)

        build(task=task, download=False)

        # Make sure that table is written
        self.assertIn('A.B.interim', Config().spark.tables)


class TestWithLocalPyspark(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.df_test = pd.DataFrame(data={'a': [1], 'b': [2]})
        Config().spark = pyspark.SparkContext()

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):

        # Detour hash_computation
        compute_hashes.return_value = {}
        task = LocalPyTask()
        build(task=task, download=False)
        outdir = task.output().path
        self.assertTrue(os.path.exists(outdir))

        outfiles = glob.glob(os.path.join(outdir, '*.parquet'))
        self.assertTrue(len(outfiles), 8)
