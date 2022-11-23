# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
import os
import pandas as pd
from luisy import ExternalTask
from luisy.decorators import (
    raw,
    csv_output,
    parquetdir_output,
)
from luisy.testing import create_testing_config


@raw
@csv_output(delimiter=';')
class SomeExternalCSVTask(ExternalTask):
    def get_file_name(self):
        return "some_export"


@raw
@csv_output(delimiter=',')
class AnotherExternalCSVTask(ExternalTask):
    def get_file_name(self):
        return "another_export"


@raw
@parquetdir_output
class ExternalParquetDir(ExternalTask):
    def get_folder_name(self):
        return 'some_dir/some_result'


class TestExternalTask(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.df = pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})

        raw_dir = os.path.join(self.tmpdir.name, 'tests', 'raw')
        some_dir = os.path.join(raw_dir, 'some_dir/some_result')
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(some_dir, exist_ok=True)
        self.df.to_csv(
            os.path.join(raw_dir, 'some_export.csv'),
            index=False,
            sep=';')

        self.df.to_csv(
            os.path.join(raw_dir, 'another_export.csv'),
            index=False,
            sep=',')

        self.df.to_parquet(
            os.path.join(some_dir, 'another_export.parquet')
        )

    def test_pickle_task(self):
        some_task = SomeExternalCSVTask()
        another_task = AnotherExternalCSVTask()

        pd.testing.assert_frame_equal(
            some_task.read(),
            another_task.read()
        )

    def test_value_error_if_method_is_not_implemented(self):
        with self.assertRaises(ValueError):
            @parquetdir_output()
            class ExternalParquetDir(ExternalTask):
                pass

    def test_read(self):
        task = ExternalParquetDir()
        pd.testing.assert_frame_equal(
            task.read(),
            self.df
        )

    def tearDown(self):
        self.tmpdir.cleanup()
