# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
import subprocess
import logging
import os
import sys
from unittest import mock

import luisy
from luisy.cli import parse_args

logger = logging.getLogger('luisy-testing')


@luisy.raw
class SomeTask(luisy.Task):
    def run(self):
        print('IM RUNNING')
        self.write(['some_output'])


@luisy.interim
@luisy.requires(SomeTask)
class SomeInterimTask(luisy.Task):

    def get_file_name(self):
        return 'ABC'

    def run(self):
        print('IM RUNING')
        self.write(['some_output'])


@luisy.interim
@luisy.requires(SomeTask)
class SomeInterimTask2(luisy.Task):

    def get_file_name(self):
        return 'ABC'

    def run(self):
        print('differentTask')
        self.write(['some_output'])


class TestCli(unittest.TestCase):

    def tearDown(self):
        self.tmpdir.cleanup()

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.requirements_file = os.path.join(self.tmpdir.name, 'requirements.txt')

        with open(self.requirements_file, 'w') as f:
            f.write('luisy==1.2.3\n')

        self.env = os.environ.copy()
        self.env['WORKING_DIR'] = self.tmpdir.name
        self.env['LUISY_AZURE_STORAGE_KEY'] = 'some secret'
        self.env['LUISY_AZURE_ACCOUNT_NAME'] = 'account name'
        self.env['LUISY_AZURE_CONTAINER_NAME'] = 'container name'
        self.env['LUISY_REQUIREMENTS_PATH'] = self.requirements_file
        self.python_cmd = sys.executable

    def assert_running(self, stdout):
        self.assertIn("IM RUNNING", stdout)
        self.assertIn(":)", stdout)

    def _run_cmdline(self, args, env=None):
        if env is None:
            env = self.env
        env['PYTHONPATH'] = env.get('PYTHONPATH', '') + ':.:tests'
        logger.info('Running: ' + ' '.join(args))  # To simplify rerunning failing tests
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()
        return p.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")

    def test_luisy_no_envs_set(self):
        self.env.pop('WORKING_DIR')
        self.env.pop('LUISY_AZURE_STORAGE_KEY')

        _, stdout, _ = self._run_cmdline(
            args=[self.python_cmd, '-m', 'luisy', '--module', 'test_cli', 'SomeTask',
                  '--local-scheduler', f'--working-dir={self.tmpdir.name}',
                  '--azure-storage-key=abc', '--no-ask'],
            env=self.env)

        self.assert_running(stdout)

        _, _, err = self._run_cmdline(
            args=[self.python_cmd, '-m', 'luisy', '--module', 'test_cli', 'SomeTask',
                  '--local-scheduler', '--no-ask']
        )
        self.assertIn('ValueError', err)
        self.assertIn("Parameter 'working_dir' not set", err)

    def test_missing_storage_key(self):
        self.env.pop('LUISY_AZURE_STORAGE_KEY')
        retcode, stdout, err = self._run_cmdline(
            args=[self.python_cmd, '-m', 'luisy', '--module', 'test_cli', 'SomeTask',
                  '--local-scheduler', '--no-ask']
        )

        self.assert_running(stdout)

        retcode, stdout, err = self._run_cmdline(
            args=[self.python_cmd, '-m', 'luisy', '--module', 'test_cli', 'SomeTask',
                  '--local-scheduler', '--download', '--no-ask']
        )

        self.assertIn("Environment variables for Azure-connection not properly set", err)

    @mock.patch.dict(os.environ, {"WORKING_DIR": "/data", 'LUISY_AZURE_STORAGE_KEY': "-"})
    def test_arg_parsing(self):
        args = ['luisy', '--module', 'test_cli', 'SomeTask', '--download', '--hash-update-mode',
                '--upload', '--no-ask', '--cloud_mode']
        self.assertDictEqual(
            parse_args(args)[0],
            {
                'download': True,
                'upload': True,
                'dry_run': False,
                'hash_update_mode': True,
                'no_ask': True,
                'cloud_mode': False,
            }
        )

    @mock.patch.dict(os.environ, {"WORKING_DIR": "/data", 'LUISY_AZURE_STORAGE_KEY': "-"})
    def test_arg_parsing_no_ask(self):
        args = ['luisy', '--module', 'test_cli', 'SomeTask', '--no-ask', '--download', '--upload',
                '--cloud_mode']
        self.assertDictEqual(
            parse_args(args)[0],
            {
                'download': True,
                'upload': True,
                'dry_run': False,
                'no_ask': True,
                'hash_update_mode': False,
                'cloud_mode': False,
            }
        )

    def test_luisy_envs_set(self):
        retcode, stdout, err = self._run_cmdline(
            args=[self.python_cmd, '-m', 'luisy', '--module',
                  'test_cli', 'SomeTask', '--local-scheduler', '--no-ask'],
            env=self.env
        )
        self.assert_running(stdout)
