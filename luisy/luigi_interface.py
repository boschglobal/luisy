# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

"""
This module contains all touch points with the package :py:mod`luigi`. We had to adapt the method
`run_luigi`, because we need to handle the system exit by ourselves.

The current logic of luisy looks looks like this:

- Do some luisy stuff like hashing every Task in the new DAG built up by given root task
- Run luigi
- Do some luisy stuff, e.g. save hashes.

"""

import luigi
import sys
from luigi.cmdline_parser import CmdlineParser
from luigi.setup_logging import InterfaceLogging
from luigi.retcodes import retcode
import logging

from luisy.testing import debug_run
from luisy.config import Config
from luisy.hashes import (
    HashSynchronizer,
    get_upstream_tasks,

)


class COLORS:
    RED = '\033[91m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    END = '\033[0m'


logger = logging.getLogger('luigi-interface').getChild("luisy-interface")


def run_luigi(argv):
    """
    Run luigi with command line parsing. This function is taken from
    :py:func:`luigi.retcodes.run_with_retcodes()`.
    We need to overwrite this,because luigi calls sys.exit(). luisy should keep on running to do
    stuff after luigi completed.

    Args:
        argv: Should (conceptually) be ``sys.argv[1:]``

    """
    logger = logging.getLogger('luigi-interface')
    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        retcodes = retcode()

    try:
        worker = luigi.interface._run(argv).worker
    except luigi.interface.PidLockAlreadyTakenExit:
        sys.exit(retcodes.already_running)
    except Exception:
        # Some errors occur before logging is set up, we set it up now
        env_params = luigi.interface.core()
        InterfaceLogging.setup(env_params)
        logger.exception("Uncaught exception in luigi")
        sys.exit(retcodes.unhandled_exception)

    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        task_sets = luigi.execution_summary._summary_dict(worker)
        root_task = luigi.execution_summary._root_task(worker)
        non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()

    def has(status):
        assert status in luigi.execution_summary._ORDERED_STATUSES
        return status in non_empty_categories

    codes_and_conds = (
        (retcodes.missing_data, has('still_pending_ext')),
        (retcodes.task_failed, has('failed')),
        (retcodes.already_running, has('run_by_other_worker')),
        (retcodes.scheduling_error, has('scheduling_error')),
        (retcodes.not_run, has('not_run')),
    )
    expected_ret_code = max(code * (1 if cond else 0) for code, cond in codes_and_conds)

    failed_ret_code = expected_ret_code == 0
    root_task_not_complete = root_task not in task_sets["completed"]
    root_task_not_already_done = root_task not in task_sets["already_done"]
    is_failed = failed_ret_code and root_task_not_complete and root_task_not_already_done
    return is_failed, expected_ret_code, task_sets, root_task


class CmdPrinter:
    def __init__(self, hash_synchronizer):
        self.hash_synchronizer = hash_synchronizer

    def print_tasks(self, text, hash_entries=None):
        if hash_entries is None:
            hash_entries = []
        print(text)
        for hash_entry in hash_entries:
            size = len(hash_entry.task.__repr__())
            print(
                COLORS.BLUE, " ", hash_entry.task, COLORS.END,
                "-" * (30 - size) + "->",
                COLORS.GREEN, hash_entry.filename, COLORS.END)

        print("\n")

    def print_opening(self, message=None):
        print(COLORS.RED, "=> Hey you! <=", COLORS.END)
        if message is not None:
            print(COLORS.RED, message, COLORS.END)

    def print_hash_changes(self):
        self.print_tasks(
            text="The following hashes are out-dated on your system:",
            hash_entries=self.hash_synchronizer.get_outdated_local_hash_entries())

    def print_new_hashes(self):
        self.print_tasks(
            text='The following hashes do not exists on your system:',
            hash_entries=self.hash_synchronizer.get_new_local_hash_entries()
        )

    def print_sources(self):
        # Not downloadable but new or outdated
        self.print_tasks(
            text="The following tasks will be executed:",
            hash_entries=self.hash_synchronizer.get_local_execution_hash_entries())
        if Config().get_param('download'):
            self.print_tasks(
                text="The following files can be downloaded from the cloud (only necessary ones "
                     "will be):",
                hash_entries=self.hash_synchronizer.get_download_hash_entries())

    def print_uploads(self):
        if Config().get_param('upload'):
            self.print_tasks(
                text="The following files will be uploaded to the cloud and existing files "
                     "overwritten",
                hash_entries=self.hash_synchronizer.get_upload_hash_entries()
            )

    def print_cloud_hash_changes(self):
        self.print_tasks(
            text="The following hashes are outdated in the cloud and will be overwritten:",
            hash_entries=self.hash_synchronizer.get_outdated_cloud_hash_entries()
        )

    def get_user_permission(self, message=None):
        """
        Asks the user if the change applied is alright. These checks can be skipped if the
        parameter `no_ask` is set.
        """
        if not Config().get_param('no_ask'):
            user_input = input("Type 'y' to proceed: ")
            if user_input.lower() != 'y':
                print(f"User input '{user_input}' != y -> Bye")
                sys.exit(0)
        else:
            logger.info('--no-ask set, wont ask anything')


class Coordinator(object):
    """

    Parent class to handle different luisy modes. The basic coordinator does hash computation of
    a given root task and reads hashes that are found on the local filesystem in `.luisy.hash`
    file.

    Args:
        args_luisy (dict): Arguments for luisy
        root_task (luisy.tasks.base.Task): Root task to be executed

    """

    def __init__(self, args_luisy, root_task=None):
        self.args_luisy = args_luisy
        self.root_task = root_task

        Config().update(self.args_luisy)
        Config().check_params()

        # Get all tasks
        self.tasks = get_upstream_tasks(self.root_task)

    def initialize_hash_synchronization(self):
        self.hash_synchronizer = HashSynchronizer(
            tasks=self.tasks,
            root_task=self.root_task
        )
        self.hash_synchronizer.initialize()

    def initialize_printer(self):
        self.printer = CmdPrinter(self.hash_synchronizer)

    def run(self):
        self.initialize_hash_synchronization()
        self.initialize_printer()
        self.print_approvals()
        self.hash_synchronizer.set_files_to_download()

        if Config().get_param('dry_run'):
            print('This is just a dry-run. Byebye!')
            return 0

        run_failed = self.run_coordinator()

        if run_failed:
            return 1
        else:
            return 0

    def run_coordinator(self):
        raise NotImplementedError()

    def print_approvals(self):
        raise NotImplementedError()


class HashUpdateMode(Coordinator):
    """
    The HashUpdateMode is started when the user passes `--hash-update` to luisy. Instead of
    rerunning all the tasks, whose hash changed in this run, this mode only updates the hashes (
    which already exist) in the `.luisy.hash` file and does not run the tasks again. This feature
    can be useful when refactoring your luisy Pipeline. It's also possible to update the hashes
    in the cloud when `--upload` is passed to luisy.
    """

    def print_approvals(self):
        self.printer.print_opening(
            message='You are running in Hash-Update-Mode! This means no '
                    'tasks will be run, downloaded or uploaded. Only the '
                    'hashes in luisy.hash files will be updated locally '
                    'and in the cloud if --download flag is set'
        )
        self.printer.print_hash_changes()
        self.printer.print_cloud_hash_changes()
        self.printer.get_user_permission()

    def run_coordinator(self):
        self.hash_synchronizer.synchronize_local_hashes(failed_tasks=[])
        if Config().get_param('upload'):
            self.hash_synchronizer.synchronize_cloud_hashes(failed_tasks=[], upload_tasks=False)


class PipelineMode(Coordinator):
    """
    This class manages the communication between luisy and luigi functionalities.

    Args:
        args_luisy (dict): Arguments for luisy
        args_luigi (list): Parsed command line arguments for luigi
        root_task (luisy.tasks.base.Task): Root task to be executed
        run_luigi_cli (bool): If true, luigi is called via command line, otherwise
            :py:func:`luisy.testing.debug_run` is called.

    """

    def __init__(self, args_luisy, args_luigi, root_task=None, run_luigi_cli=True):
        self.args_luigi = args_luigi
        self.run_luigi_cli = run_luigi_cli

        super().__init__(args_luisy=args_luisy, root_task=root_task)

    def run_luigi(self):

        if self.run_luigi_cli:
            luigi_failed, _, self.tasks_set, _ = run_luigi(self.args_luigi)
            return luigi_failed
        else:
            print('RUNNING')
            print(self.root_task)
            self.tasks_set = debug_run([self.root_task], return_summary=True)
            return len(self.tasks_set['failed']) > 0

    def finalize_luigi_run(self):
        self.hash_synchronizer.synchronize_local_hashes(failed_tasks=self.tasks_set['failed'])
        if Config().get_param('upload'):
            self.hash_synchronizer.synchronize_cloud_hashes(failed_tasks=self.tasks_set['failed'])

    def run_coordinator(self):
        self.hash_synchronizer.clear_outdated_tasks()
        luigi_failed = self.run_luigi()
        self.finalize_luigi_run()
        return luigi_failed

    def print_approvals(self):
        self.printer.print_opening('You are in Pipeline Mode!')
        self.printer.print_hash_changes()
        self.printer.print_new_hashes()
        self.printer.print_sources()
        self.printer.print_uploads()
        self.printer.get_user_permission()


def get_root_task(args_luigi):
    with CmdlineParser.global_instance(args_luigi) as cp:
        return cp.get_task_obj()
