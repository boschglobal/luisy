# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import luigi
import tempfile
import os
import sys
import pandas as pd
from unittest.mock import patch

from luisy import (
    Task,
    ExternalTask,
    ConcatenationTask,
    requires,
    WrapperTask,
)
from luisy.decorators import (
    raw,
    interim,
    final,
    pickle_output,
    hdf_output,
    csv_output,
    make_directory_output,
)
from luisy.hashes import (
    compute_hashes,
    get_upstream_tasks,
    remove_working_dir,
)
from luisy.code_inspection import (
    get_requirements_path,
    get_requirements_dict,
)
from luisy.testing import (
    create_testing_config,
    luisyTestCase,
    debug_run,
)


@raw
class ToyLowestTask(Task):
    def run(self):
        self.write({'A': 2})


@interim
@requires(ToyLowestTask)
class ToyTask(Task):
    a = luigi.IntParameter()
    b = luigi.IntParameter(default=2)

    def run(self):
        data = {'A': self.a, 'B': self.b}
        self.write(data)


@final
@hdf_output
class ToyHDFTask(Task):
    a = luigi.IntParameter(default=1)
    task_namespace = 'ABC'

    def run(self):
        df = pd.DataFrame(
            data={
                'A': [1, 2, 3],
                'B': [4, 5, 6]
            }
        )
        df = self.a * df
        self.write(df)


@final
@pickle_output
@requires(ToyHDFTask)
class SubsequentTask(Task):

    def run(self):
        df = self.input().read()
        df = df * 5
        self.write(df)


@interim
@requires(ToyHDFTask, ToyTask, as_dict=True)
class MultipleRequires(Task):

    def run(self):
        df_1 = self.input()['ABC.ToyHDFTask'].read()
        df_2 = self.input()['ToyTask'].read()
        self.write([df_1, df_2])


class SomeConcatenation(ConcatenationTask):
    a_list = luigi.ListParameter(default=[1, 2, 3])

    def requires(self):
        for a in self.a_list:
            yield ToyHDFTask(a=a)


class SomeWrapping(WrapperTask):
    a_list = luigi.ListParameter(default=[1, 2, 3])

    def requires(self):
        for a in self.a_list:
            yield ToyHDFTask(a=a)


class SomeDictWrapping(WrapperTask):
    a_list = luigi.ListParameter(default=[1, 2, 3])

    def requires(self):
        return {
            a: ToyHDFTask(a=a) for a in self.a_list
        }


class BaseTaskTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()


class TestTask(BaseTaskTestCase):
    def test_pickle_task(self):
        task = ToyTask(a=1, b=2)
        task.run()
        self.assertDictEqual(
            task.read(),
            {'A': 1, 'B': 2}
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'interim',
                    'ToyTask_a=1_b=2.pkl'
                )
            )
        )

    def test_hdf_task(self):
        task = ToyHDFTask(a=1)
        task.run()

        df = task.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'final',
                    'ABC.ToyHDFTask_a=1.hdf'
                )
            )
        )

    def test_parameter_none(self):
        task = ToyTask(a=None, b=1)
        task.run()
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'interim',
                    'ToyTask_a=None_b=1.pkl'
                )
            )
        )

    def test_read_of_input(self):
        task = SubsequentTask(a=1)
        luigi.build([task], local_scheduler=True)

        df = task.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'final',
                    'SubsequentTask_a=1.pkl'
                )
            )
        )

    def test_multiple_requirements(self):
        task = MultipleRequires()
        luigi.build([task], local_scheduler=True)
        result = task.read()
        self.assertIsInstance(result[0], pd.DataFrame)
        self.assertIsInstance(result[1], dict)

    def test_project_name(self):
        @raw
        class SomeTask(ExternalTask):
            pass

        task = SomeTask()
        self.assertEqual(task.project_name, 'tests')

    @unittest.skipIf(sys.version_info < (3, 9, 0), "Hashes depend on python version")
    def test_hashes(self):
        task = MultipleRequires()

        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.7"

        with patch("luisy.code_inspection.get_requirements_dict", return_value=return_value):
            hashes = compute_hashes({"some_file": task})

        self.assertDictEqual(
            {'some_file': 'f6dd96daa0121ea687f468809737c57e'},
            hashes,
        )

    def test_get_dag_of_root_task(self):

        tasks = get_upstream_tasks(MultipleRequires())

        self.assertIsInstance(tasks, dict)
        self.assertSetEqual(
            set(tasks.values()),
            {
                ToyHDFTask(a=1),
                ToyLowestTask(),
                MultipleRequires(),
                ToyTask(a=1, b=2)
            }
        )

        upstream_task = ToyLowestTask()
        self.assertEqual(
            tasks[remove_working_dir(upstream_task.output().path)],
            upstream_task
        )

    def test_truncated_file_ending(self):
        @final
        @pickle_output
        class TaskA(Task):
            def run(self):
                self.write({"spam": ["spam"]})

            def get_file_name(self):
                return "my_pickle.pkl"

        task = TaskA()
        luigi.build([task], local_scheduler=True)

        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'final',
                    'my_pickle.pkl'
                )
            )
        )

        @final
        @pickle_output
        class TaskB(Task):
            def run(self):
                self.write({"spam": ["spam"]})

            def get_file_name(self):
                return "my_pickle_pkl"

        task = TaskB()
        luigi.build([task], local_scheduler=True)

        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'final',
                    'my_pickle_pkl.pkl'
                )
            )
        )

    def test_different_file_extension_ending(self):
        @final
        @pickle_output
        class TaskA(Task):
            def run(self):
                self.write({"spam": ["spam"]})

            def get_file_name(self):
                return "my_pickle.xlsx"

        task = TaskA()
        luigi.build([task], local_scheduler=True)

        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.tmpdir.name,
                    'tests',
                    'final',
                    'my_pickle.xlsx.pkl'
                )
            )
        )


class TestExternalTask(luisyTestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.df = pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})

        raw_dir = os.path.join(self.tmpdir.name, 'tests', 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        self.df.to_csv(os.path.join(raw_dir, 'some_export.csv'), index=False)
        create_testing_config(working_dir=self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_csv_output(self):
        @raw
        @csv_output(usecols=[0])
        class SomeExternalCSVTask(ExternalTask):
            def get_file_name(self):
                return "some_export"

        task = SomeExternalCSVTask()

        pd.testing.assert_frame_equal(
            self.df[['A']],
            task.read(),
        )

    def test_directory_output(self):
        def _read_file(file):
            return pd.read_csv(file)

        csv_directory_output = make_directory_output(
            read_file_func=_read_file,
            regex_pattern='.csv'
        )

        @raw
        @csv_directory_output
        class SomeExternalDirectory(ExternalTask):
            def get_folder_name(self):
                return ''

        files_read = []
        task = SomeExternalDirectory()
        for file in task.read():
            files_read.append(file)
        self.assertEqual(len(files_read), 1)

        @requires(SomeExternalDirectory)
        @interim
        class TestTask(Task):
            def run(self):
                files = []
                for df in self.input().read():
                    files.append(df)
                self.write(files)

        task = TestTask()
        debug_run(tasks=[task])
        files = task.read()
        self.assertEqual(len(files), 1)
        self.assertIsInstance(files[0], pd.DataFrame)


class TestConcatenationTask(BaseTaskTestCase):
    def test_concat(self):
        task = SomeConcatenation()
        luigi.build([task], local_scheduler=True)
        df = task.read()
        self.assertTupleEqual(df.shape, (3 * 3, 2))

    def test_outfile_with_lists(self):
        task = SomeConcatenation()
        self.assertEqual(
            task.output().path,
            os.path.join(self.tmpdir.name, 'SomeConcatenation_a_list=1_2_3.pkl')
        )


class TestWrapperTask(BaseTaskTestCase):
    def test_wrap(self):
        test_parameters = [1, 2, 3]
        task = SomeWrapping(a_list=test_parameters)
        luigi.build([task], local_scheduler=True)
        for param in test_parameters:
            df = ToyHDFTask(param).read()
            self.assertTupleEqual(df.shape, (3, 2))

    def test_list_read(self):
        test_parameters = [1, 2, 3]
        task = SomeWrapping(a_list=test_parameters)
        luigi.build([task], local_scheduler=True)

        output = task.read()
        self.assertIsInstance(output, list)

        for i, param in enumerate(test_parameters):
            df = ToyHDFTask(param).read()
            pd.testing.assert_frame_equal(df, output[i])

    def test_list_dict(self):
        test_parameters = [1, 2, 3]
        task = SomeDictWrapping(a_list=test_parameters)
        luigi.build([task], local_scheduler=True)

        output = task.read()
        self.assertIsInstance(output, dict)

        for param in test_parameters:
            df = ToyHDFTask(param).read()
            pd.testing.assert_frame_equal(df, output[param])
