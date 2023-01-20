# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import luigi
import pickle
import copy
from luigi.tools.deps import get_task_requires
from contextlib import ExitStack

from luisy import Config
from luisy.config import get_default_params

from luigi.execution_summary import LuigiStatusCode


from unittest.mock import (
    patch,
    Mock,
)


def mock_target(task, output_store):

    target = Mock()

    def read():
        return copy.deepcopy(output_store[task])

    def exists():
        return task in output_store

    def write(obj):
        output_store[task] = obj

    target.read = read
    target.exists = exists
    target.write = write

    return patch.object(task, "output", return_value=target)


def debug_run(tasks, return_summary=True):
    """
    This is a helper Method which allows to run pipelines in tests and bypassing cli. This method
    returns some useful information from the build as well in form of a dict.

    Args:
        tasks(list or luigi.Task): List of Tasks that should be run or a single
            :py:class:`luigi.Task`

    Returns:
        dict: Dictionary with information about the executed tasks. I.e. dict['completed']
            contains the tasks that have been completed in this run or dict['already_done']
            contains tasks that have been completed before running luigi
    """
    if isinstance(tasks, luigi.Task):
        tasks = [tasks]
    result = luigi.build(tasks, detailed_summary=True, local_scheduler=True)

    if return_summary:
        return luigi.execution_summary._summary_dict(result.worker)
    else:
        return result


def get_all_dependencies(task):

    deps = get_task_requires(task)
    for task_ in deps:
        deps = deps.union(get_all_dependencies(task_))

    deps.add(task)
    return deps


def create_file(file, val):
    with open(file, 'wb') as f:
        pickle.dump(val, f)


class LuisyTestCase(unittest.TestCase):

    def get_execution_summary(self, task, existing_outputs=None):
        result, _ = self._run_pipeline(task=task, existing_outputs=existing_outputs)
        return luigi.execution_summary._summary_dict(result.worker)

    def _run_pipeline(self, task, existing_outputs=None):
        """
        Runs the given task after providing the existing outputs.
        """

        create_testing_config(working_dir='/some/working_dir/')

        if existing_outputs is None:
            existing_outputs = []

        output_store = {
            t: o for t, o in existing_outputs
        }

        all_tasks = get_all_dependencies(task)

        patches = [mock_target(t, output_store) for t in all_tasks]

        with ExitStack() as stack:
            for patch_ in patches:
                stack.enter_context(patch_)

            result = debug_run([task], return_summary=False)
            return result, output_store

    def run_pipeline(self, task, existing_outputs=None, return_store=False):
        """
        Method that can be used to run luigi pipelines. You are able to pass existing outputs
        which mock a specific task's output. This helps the user in testing without mocking his
        task output or creating a temporary directory.

        Args:
            task (luigi.Task): Task that should be run
            existing_outputs (list): List of Tuples, where first element of tuple is the
                Taskobject whose output should be mocked and the second element is the content of
                the mocked task
            return_store (bool): If true, the whole store gets returned as a dict. The store
                contains all the outputs after the execution of the pipeline, where the tasks are
                the keys and the values are their outputs. If false, only the output of the given
                task is returned.

        Returns:
            dict or object: depending on `return_store` value, a dict with output of all tasks or
                just the output of the root_task
        """
        result, output_store = self._run_pipeline(task=task, existing_outputs=existing_outputs)

        if not result.status == LuigiStatusCode.SUCCESS:
            result_dict = luigi.execution_summary._summary_dict(result.worker)
            self.fail(result_dict)

        if return_store:
            return output_store
        else:
            return output_store[task]

    def assertSuccess(self, task, existing_outputs=None):
        result, _ = self._run_pipeline(task=task, existing_outputs=existing_outputs)
        self.assertEqual(result.status, LuigiStatusCode.SUCCESS)

    def assertFail(self, task, existing_outputs=None):
        result, _ = self._run_pipeline(task=task, existing_outputs=existing_outputs)
        self.assertNotEqual(result.status, LuigiStatusCode.SUCCESS)

    def assertMissing(self, task, existing_outputs=None):
        result, _ = self._run_pipeline(task=task, existing_outputs=existing_outputs)
        self.assertEqual(result.status, LuigiStatusCode.MISSING_EXT)


def create_testing_config(working_dir, storage_key='', container_name='', account_name='',
                          **kwargs):
    """
    Creates a Config Object for testing purpose. Should be used when working with temporary
    directories.

    Args:
        working_dir (str): working dir for testing luisy tasks (mostly tmpdirs)
        storage_key (str): storage_key that can be set if no environ variable can be used in tests.
        account_name (str): Account name that can be set if no environ variable can be used in
            tests.
        container_name (str): Container name that can be set if no environ variable can be used in
            tests.
        **kwargs: params like `download` or `upload`
    """
    params = get_default_params(raw=False)
    params['working_dir'] = working_dir
    params['azure_storage_key'] = storage_key
    params['azure_container_name'] = container_name
    params['azure_account_name'] = account_name
    for key, val in kwargs.items():
        params[key] = val
    Config().update(params)
