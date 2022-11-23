# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest import mock
import os
from luisy.config import (
    pass_args,
    Config,
    activate_download,
    set_working_dir,
    add_working_dir,
    remove_working_dir,
)


class TestConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env_patcher = mock.patch.dict(
            os.environ,
            {
                "WORKING_DIR": "/data",
                "LUISY_AZURE_STORAGE_KEY": "-",
                "LUISY_AZURE_ACCOUNT_NAME": "account name",
                "LUISY_AZURE_CONTAINER_NAME": "container name"
            }
        )
        cls.env_patcher.start()
        super().setUpClass()

    def setUp(self):
        Config().reset()
        self.params = {
            'download': False,
            'upload': True,
            'dry-run': True,
            'working_dir': 'ABC'
        }
        pass_args(args=self.params)

    def test_param(self):
        with self.assertRaises(ValueError):
            Config().get_param('A')
        self.assertFalse(Config().get_param('download'))
        self.assertEqual(Config().get_param('working_dir'), 'ABC')

    def test_add_param(self):
        Config().set_param('A', 10)
        self.assertEqual(Config().get_param('A'), 10)

    def test_env_variables(self):
        Config().reset()
        self.assertEqual(Config().get_param('working_dir'), '/data')
        self.assertEqual(Config().get_param('azure_storage_key'), '-')

    def test_not_set(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            Config().reset()
            self.assertIsNone(Config().get_param('working_dir'))

    def test_singleton(self):
        c1 = Config()
        c2 = Config()
        self.assertEqual(c1, c2)

    def test_activate_download(self):
        Config().reset()
        self.assertFalse(Config()._config['download'])
        activate_download()
        self.assertTrue(Config()._config['download'])

    def test_set_working_dir(self):
        self.assertEqual(Config()._config['working_dir'], 'ABC')
        set_working_dir('notABC')
        self.assertEqual(Config()._config['working_dir'], 'notABC')

    def test_add_working_dir(self):
        self.assertEqual(
            add_working_dir("/some/path"),
            "ABC/some/path"
        )

    def test_remove_working_dir(self):
        self.assertEqual(
            remove_working_dir("ABC/some/path"),
            "/some/path"
        )
