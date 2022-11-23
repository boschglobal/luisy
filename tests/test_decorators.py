# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock

import luigi
import luisy

from luisy import Task
from luisy.testing import luisyTestCase
from luisy.decorators import (
    _make_output_decorator_factory,
    auto_filename
)


class MyTask(Task):
    a = luigi.IntParameter()
    b = luigi.IntParameter()
    c = luigi.IntParameter()


class MyTarget(luigi.LocalTarget):

    def read(self):
        pass


@luisy.decorators.json_output
class MyJSONTask(Task):
    def run(self):
        dct = {
            'A': 'b',
            'C': 'd'
        }
        self.write(dct)


class TestTargetDecorators(unittest.TestCase):

    def test_decorator_factory_with_kwargs(self):
        make_decorator = _make_output_decorator_factory(MyTarget)
        decorator = make_decorator(a=2, b=1)
        mock = MagicMock()
        mock = decorator(mock)
        self.assertDictEqual(
            mock.target_kwargs,
            {'a': 2, 'b': 1}
        )


class TestDecoratorOutputs(luisyTestCase):
    def test_json_decorator(self):
        dct = self.run_pipeline(MyJSONTask())
        self.assertDictEqual(
            d1=dct,
            d2={
                'A': 'b',
                'C': 'd'
            }
        )


class TestAutoFilename(unittest.TestCase):

    def test_get_filename(self):
        DecoratedTask = auto_filename(MyTask)
        self.assertEqual(
            DecoratedTask(a=1, b=2, c=3).get_file_name(),
            'MyTask_a=1_b=2_c=3'
        )

    def test_auto_filename_exclude_params(self):
        DecoratedTask = auto_filename(MyTask, excl_params=['a'])
        self.assertEqual(
            DecoratedTask(a=1, b=2, c=3).get_file_name(),
            'MyTask_b=2_c=3'
        )

    def test_auto_filename_exclude_None(self):
        DecoratedTask = auto_filename(MyTask, excl_none=True)
        self.assertEqual(
            DecoratedTask(a=1, b=None, c=3).get_file_name(),
            'MyTask_a=1_c=3'
        )

    def test_decorator_with_parenthesis(self):
        @auto_filename()
        class MyTask1(Task):
            a = luigi.IntParameter()
            b = luigi.IntParameter()
            c = luigi.IntParameter()

        DecoratedTask = auto_filename(MyTask1)
        self.assertEqual(
            DecoratedTask(a=1, b=2, c=3).get_file_name(),
            'MyTask1_a=1_b=2_c=3'
        )


class TestDecoratorParenthesis(luisyTestCase):

    def test_output_decorator_factory_parenthesis(self):
        @luisy.decorators.csv_output()
        class MyCSVTaskWithParenthesis(Task):
            pass

        @luisy.decorators.csv_output
        class MyCSVTaskWithoutParenthesis(Task):
            pass

        self.assertEqual(
            MyCSVTaskWithParenthesis().get_file_name(),
            'MyCSVTaskWithParenthesis'
        )
        self.assertEqual(
            MyCSVTaskWithoutParenthesis().get_file_name(),
            'MyCSVTaskWithoutParenthesis'
        )

    def test_output_decorator_parenthesis(self):
        @luisy.decorators.hdf_output()
        class MyHDFTaskWithParenthesis(Task):
            pass

        @luisy.decorators.hdf_output
        class MyHDFTaskWithoutParenthesis(Task):
            pass

        self.assertEqual(
            MyHDFTaskWithParenthesis().get_file_name(),
            'MyHDFTaskWithParenthesis'
        )
        self.assertEqual(
            MyHDFTaskWithoutParenthesis().get_file_name(),
            'MyHDFTaskWithoutParenthesis'
        )

    def test_dir_decorator_parenthesis(self):
        @luisy.decorators.final()
        @luisy.decorators.pickle_output
        class TaskWithParenthesis(Task):
            def run(self):
                self.write([0])

        @luisy.decorators.final
        @luisy.decorators.pickle_output
        class TaskWithoutParenthesis(Task):
            def run(self):
                self.write([0])

        task_a = TaskWithParenthesis()
        self.run_pipeline(task_a)
        self.assertEqual(
            task_a.get_outdir(),
            '/some/working_dir/tests/final/',
        )
        task_b = TaskWithoutParenthesis()
        self.run_pipeline(task_b)
        self.assertEqual(
            task_b.get_outdir(),
            '/some/working_dir/tests/final/',
        )
