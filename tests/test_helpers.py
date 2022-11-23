# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
import os

import luisy
from luisy.helpers import (
    get_string_repr,
    RegexTaskPattern
)
from luisy.testing import (
    create_testing_config,
    debug_run
)
from luisy.config import change_working_dir


class TestHelpers(unittest.TestCase):

    def test_str_repr(self):
        self.assertEqual(
            get_string_repr(['a', 1, 'c']),
            'a_1_c'
        )

    def test_str_nested_list(self):
        self.assertEqual(
            get_string_repr(['a', [1, 2, 3], 'c']),
            'a_1_2_3_c'
        )

    def test_str_slash(self):
        self.assertEqual(
            get_string_repr(['a/b', 'b/c', 'c/d']),
            'a_b_b_c_c_d'
        )

    def test_change_working_dir(self):
        path = '/my_working_dir/raw/file.hdf'
        self.assertEqual(
            change_working_dir(path, '/my_working_dir', '/my_new_working_dir'),
            '/my_new_working_dir/raw/file.hdf'
        )

    def test_change_working_dir_with_enclosing_slash(self):
        path = '/my_working_dir/raw/file.hdf'
        self.assertEqual(
            change_working_dir(path, '/my_working_dir/', '/my_new_working_dir'),
            '/my_new_working_dir/raw/file.hdf'
        )

        self.assertEqual(
            change_working_dir(path, '/my_working_dir', '/my_new_working_dir/'),
            '/my_new_working_dir/raw/file.hdf'
        )

    def test_removal_of_working_dir(self):
        path = '/my_working_dir/raw/file.hdf'
        self.assertEqual(
            change_working_dir(path, '/my_working_dir/', ""),
            '/raw/file.hdf'
        )

        self.assertEqual(
            change_working_dir(path, '/my_working_dir', ""),
            '/raw/file.hdf'
        )


@luisy.interim
class CustomFilename(luisy.Task):
    a = luisy.IntParameter()
    b = luisy.IntParameter()
    c = luisy.IntParameter()

    def get_sub_dir(self):
        return os.path.join(
            'this',
            str(self.a),
            'isjustatest'
        )

    def get_file_name(self):
        return f'{self.a}_fake_filename_{self.b}_{self.c}'

    def run(self):
        self.write('')


@luisy.final
@luisy.requires(CustomFilename)
@luisy.auto_filename
class MyTask2(luisy.Task):
    a = luisy.IntParameter(default=1)
    b = luisy.IntParameter(default=1)
    c = luisy.IntParameter(default=1)

    def run(self):
        self.write(self.a)


@luisy.final
@luisy.requires(CustomFilename)
class MessedFilename(luisy.Task):
    a = luisy.IntParameter(default=1)
    b = luisy.IntParameter(default=1)
    c = luisy.IntParameter(default=1)

    def get_file_name(self):
        return f'1_1_1_1_1_{self.a}'

    def run(self):
        self.write(self.a)


class TestRegexTaskPattern(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmp_dir.name)
        self.task1 = MyTask2()
        self.task2 = MyTask2(a=2, b=2, c=2)
        self.task3 = MyTask2(a=2, b=3, c=2)
        self.task5 = MessedFilename(a=2, b=2, c=1)
        debug_run(tasks=[self.task1, self.task2, self.task3, self.task5])
        self.task4 = CustomFilename(a=2, b=2, c=2)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_get_related_instances(self):
        tasks = MyTask2.get_related_instances()
        self.assertEqual(len(tasks), 3)
        for task in tasks:
            number = task.read()
            self.assertEqual(
                number,
                int(task.a)
            )

        tasks = CustomFilename.get_related_instances()
        self.assertEqual(len(tasks), 4)
        with self.assertRaises(Exception):
            MessedFilename.get_related_instances()

    def test_root_directory(self):
        regex_pattern = RegexTaskPattern(CustomFilename)
        root_dir = regex_pattern.build_root_directory()
        desired_root_dir = os.path.join(self.tmp_dir.name, 'tests', 'interim', 'this')
        self.assertEqual(desired_root_dir, root_dir)

    def test_context_manager(self):
        regex_pattern = RegexTaskPattern(MyTask2)
        with regex_pattern._overwrite_defaults() as task:
            context_task = task()
            self.assertEqual(context_task.a, '*a*')
            self.assertEqual(context_task.b, '*b*')
            self.assertEqual(context_task.c, '*c*')
        new_task = MyTask2()
        self.assertEqual(new_task.a, 1)
        self.assertEqual(new_task.b, 1)
        self.assertEqual(new_task.c, 1)

    def test_regex(self):
        regex_pattern = RegexTaskPattern(CustomFilename)
        regex = regex_pattern.regex_pattern
        self.assertEqual(
            fr'({self.tmp_dir.name}/tests/interim/this/)([\S]*)(/isjustatest/)([\S]*)'
            fr'(_fake_filename_)([\S]*)(_)([\S]*)(.pkl)',
            regex
        )
