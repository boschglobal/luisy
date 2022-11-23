# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import json
import hashlib
import luigi
import tempfile
import os
import pandas as pd

from luisy import (
    Task,
    ExternalTask,
    requires,
)

from luisy.decorators import (
    raw,
    interim,
    final,
    csv_output,
)
from luisy.hashes import remove_working_dir
from luisy.testing import (
    create_testing_config,
    debug_run,
)

from luisy import Config
from luisy.cli import build


@raw
@csv_output()
class RawTask(ExternalTask):
    def get_file_name(self):
        return 'raw_file'


@interim
@requires(RawTask)
class InterimTask(Task):
    a = luigi.IntParameter()

    def run(self):
        df = self.input().read()
        df = self.a * df
        self.write(df)


@final
@requires(InterimTask)
class FinalTask(Task):
    a = luigi.IntParameter(default=1)

    def run(self):
        df = self.input().read()
        df = self.a + df
        self.write(df)


class PureLuigiTask(luigi.Task):

    path = luigi.Parameter()
    a = luigi.IntParameter(default=1)

    def run(self):
        df = self.input().read()
        df = df / self.a
        df.to_csv(self.output().path)

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return FinalTask(a=self.a)


@final
class PureluisyTask(Task):
    a = luigi.IntParameter(default=1)

    def run(self):
        df = pd.read_csv(self.input().path)
        df = self.a + df
        self.write(df)

    def requires(self):
        return PureLuigiTask(
            a=self.a,
            path=os.path.join(self.get_outdir(), 'pure_task.csv')
        )


class TestHashes(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

        self.requirements_file = os.path.join(self.tmpdir.name, 'requirements.txt')
        self.hash_file = os.path.join(self.tmpdir.name, 'tests', '.luisy.hash')

        with open(self.requirements_file, 'w') as f:
            f.write('luisy==1.2.3\n')
            f.write('luigi==3.2.1\n')
            f.write('pandas==100.0.0\n')

        RawTask().write(
            pd.DataFrame(
                data={
                    'a': [1, 2],
                    'b': [3, 4]
                }
            )
        )

    def read_hash_file(self):
        with open(self.hash_file, 'r') as f:
            return json.load(f)

    def update_hash_file(self, hashes):
        with open(self.hash_file, 'w+') as f:
            json.dump(hashes, f)

    def test_compatibility_with_luigi_run(self):
        task = FinalTask(a=2)
        result = debug_run(task)

        self.assertIn(RawTask(), result['already_done'])
        self.assertIn(InterimTask(a=2), result['completed'])
        self.assertIn(FinalTask(a=2), result['completed'])

        df = task.read()
        self.assertIsInstance(df, pd.DataFrame)

    def test_hash_creation(self):

        build(
            task=FinalTask(a=2),
            requirements_path=self.requirements_file
        )

        self.assertTrue(os.path.exists(self.hash_file))
        hashes = self.read_hash_file()

        self.assertIn(remove_working_dir(FinalTask(a=2).output().path), hashes)
        self.assertIn(remove_working_dir(InterimTask(a=2).output().path), hashes)
        self.assertIn(remove_working_dir(RawTask().output().path), hashes)

    def test_rerun_on_hash_changes(self):

        task_interim = InterimTask(a=2)
        task_final = FinalTask(a=2)

        coordinator = build(
            task=FinalTask(a=2),
            requirements_path=self.requirements_file
        )
        self.assertIn(RawTask(), coordinator.tasks_set['already_done'])
        self.assertIn(InterimTask(a=2), coordinator.tasks_set['completed'])
        self.assertIn(FinalTask(a=2), coordinator.tasks_set['completed'])

        # Change hashes to simulate a code change
        # Here, we have to change both hashes as our code_inspection checks for the actual code
        # (which hasn't changed)
        hashes = self.read_hash_file()

        file_interim = remove_working_dir(task_interim.output().path)
        file_final = remove_working_dir(task_final.output().path)

        hashes_changed = {
            file_interim: hashlib.md5('some_other_hash'.encode()).hexdigest(),
            file_final:  hashlib.md5('some_other_hash'.encode()).hexdigest(),
        }

        self.update_hash_file(hashes_changed)

        # Reset luigis cache
        FinalTask.clear_instance_cache()
        InterimTask.clear_instance_cache()

        # Rerun with luisy
        coordinator = build(
            task=FinalTask(a=2),
            requirements_path=self.requirements_file
        )

        # Check that both tasks have been executed
        self.assertIn(InterimTask(a=2), coordinator.tasks_set['completed'])
        self.assertIn(FinalTask(a=2), coordinator.tasks_set['completed'])

        self.assertDictEqual(
            hashes,
            self.read_hash_file()
        )

    def test_dry_run(self):
        # Rerun with luisy

        Config().set_param('dry_run', True)
        build(
            task=FinalTask(a=3),
            requirements_path=self.requirements_file
        )

        self.assertFalse(FinalTask(a=3).complete())
        Config().set_param('dry_run', False)

    def test_compatibility_with_intermediate_luigi_tasks(self):

        task = PureluisyTask(a=1)

        coordinator = build(
            task=task,
            requirements_path=self.requirements_file
        )

        self.assertIn(task, coordinator.tasks_set['completed'])

    def test_multiple_workers(self):
        task = FinalTask(a=2)
        coordinator = build(
            task=task,
            requirements_path=self.requirements_file,
            workers=1,

        )
        self.assertIn(task, coordinator.tasks_set['completed'])

    def tearDown(self):
        self.tmpdir.cleanup()
