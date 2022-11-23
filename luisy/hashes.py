# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import json

from luisy.code_inspection import create_hashes
from luisy.testing import get_all_dependencies
from luisy import (
    Task,
    ExternalTask,
)
from luisy import Config
from luisy.targets import ParquetDirTarget
from luisy.config import (
    add_working_dir,
    remove_working_dir,
    change_working_dir,
)

logger = logging.getLogger(__name__)


class TaskHashEntry:
    """
    This class manages all available hash entries in a luisy run. Each task of luisy has its own
    hash depending on the code version. So in one luisy run, a task can have multiple hashes:
    - hash in local `luisy.hash` file
    - hash in cloud `luisy_hash` file (cloud)
    - hash that was computed in this current run

    Most of the methods of this class do comparisons between those hashes to give luisy an
    idea of what needs to be executed, downloaded, uploaded and which hashes need to be refreshed
    in `.luisy.hash` file online or locally.

    Args:
          hash_new (str): hash of current luisy run
          hash_local (str or None): hash of local 'luisy.hash' file (if available)
          hash_cloud (str or None): hash of cloud 'luisy.hash' file (if available)
          task (luisy.Task): instance of task which all the hashes belong to
          filename (str): output filename of task (key inside `luisy.hash` file)

    Note:
        Can this be moved inside a Task instance?

    """

    def __init__(self, hash_new, hash_local=None, hash_cloud=None, task=None, filename=None):
        self.hash_new = hash_new
        self.hash_local = hash_local
        self.hash_cloud = hash_cloud
        self.task = task
        self.filename = filename

    def in_cloud(self):
        """
        Returns:
            bool: if Task has a hash saved in the cloud
        """
        return self.hash_cloud is not None

    def has_outdated_cloud_hash(self):
        """
        Returns:
            bool: if Task has a hash saved in the cloud and this is outdated
        """
        return self.hash_cloud != self.hash_new and self.in_cloud()

    def in_local(self):
        return self.hash_local is not None

    def is_downloadable(self):
        return self.in_cloud() and not self.has_outdated_cloud_hash()

    def has_outdated_local_hash(self):
        return self.hash_local != self.hash_new and self.in_local()

    def is_external_task(self):
        return isinstance(self.task, ExternalTask)

    def has_parquet_output(self):
        return issubclass(self.task.target_cls, ParquetDirTarget)

    def needs_local_execution(self):
        return (self.has_outdated_local_hash() or not self.in_local()) and not \
            self.is_downloadable()

    def needs_to_be_downloaded(self):
        return (self.has_outdated_local_hash() or not self.in_local()) and self.is_downloadable()


class HashSynchronizer(object):
    """
    Class to keep hashmappings of a luisy run synched. Calling the HashSynchronizer.initialize()
    loads the following Hashmappings, that will be managed in this class:
    - current run of luisy
    - local `.luisy.hash`
    - cloud `.luisy.hash`

    This class manages the synchronization between those hashmappings when luisy did some updates
    on them. It also offers some functions that gives the user information on which tasks to
    download/upload/rerun/...

    Args:
        tasks(dict): values are Tasks of current luisy run, keys are corresponding filenames.
        root_task(luisy.Task): Root task of the current luisy run (needed to get `project name`)

    """

    def __init__(self, tasks, root_task):
        self.tasks = tasks
        self.root_task = root_task
        self.hash_entries = []

    def check_params(self):
        assert self.root_task in self.tasks.values()
        task_filesnames = [remove_working_dir(task.output().path) for task in self.tasks.values()]
        assert all([filename in task_filesnames for filename in self.hashmapping_new.hashes.keys()])

    def initialize(self):
        """
        Initializes all hashmappings needed in this class:
        - hash_mapping_new: computed by current pipeline
        - hash_mapping_local: loaded from local `luisy.hash` file
        - hash_mapping_cloud: loaded from `luisy.hash` file in cloud

        Also sets up list of hash_entries, which are used to compare hashmappings.

        """
        self._initialize_hashmappings()
        hashes_new = self.hashmapping_new.hashes
        hashes_local = self.hashmapping_local.hashes
        hashes_cloud = self.hashmapping_cloud.hashes
        self.check_params()
        for filename in hashes_new.keys():
            self.hash_entries.append(TaskHashEntry(
                hash_new=hashes_new[filename] if filename in hashes_new.keys() else None,
                hash_local=hashes_local[filename] if filename in hashes_local.keys() else None,
                hash_cloud=hashes_cloud[filename] if filename in hashes_cloud.keys() else None,
                task=self.tasks[filename] if filename in self.tasks.keys() else None,
                filename=filename,
            ))

    def _initialize_hashmappings(self):
        self.hashmapping_new = self.compute_new_hashes()
        self.hashmapping_local = self.read_local_hash_file()
        self.hashmapping_cloud = self.read_cloud_hash_file()
        self._add_external_cloud_hashes()

    def compute_new_hashes(self):
        logger.info('Compute hashes of tasks')
        return HashMapping.compute_from_tasks(
            tasks=self.tasks,
            project_name=self.root_task.project_name
        )

    def read_local_hash_file(self):
        logger.info('Read Hashes from disk')
        hashes = HashMapping.load_from_local(
            project_name=self.root_task.project_name
        )
        hashes.clean_up()
        return hashes

    def read_cloud_hash_file(self):
        if Config().get_param('download') or Config().get_param('upload'):
            logger.info('Read Hashes from cloud')
            hashes = HashMapping.load_from_cloud(
                project_name=self.root_task.project_name
            )
            hashes.clean_up()
            return hashes
        else:
            return HashMapping(hashes={}, local=False, project_name=self.root_task.project_name)

    def _add_external_cloud_hashes(self):
        """

        It might be that some files in the cloud are added by a none-luisy system, like a Databricks
        cluster that writes some parquets to the blob-storage correctly. Those files cannot be
        versioned with luisy and we have to assume that these files are immutable. Thus, we will
        check if any tasks in the pipeline point to such files and will add their hashes.
        """
        for filename, task in self.tasks.items():
            if issubclass(task.target_cls, ParquetDirTarget):

                if self.hashmapping_cloud.get_hash(filename) is not None:
                    continue

                self.hashmapping_cloud.update(
                    filepath=filename,
                    task_hash=self.hashmapping_cloud.get_hash(filename)
                )

    def get_outdated_local_hash_entries(self):
        """
        Out dated tasks without external tasks as external tasks should not be reran at all

        Returns:
            list: hash entries of tasks that are outdated (no external tasks included)

        """

        return [
            hash_entry for hash_entry in self.hash_entries if
            hash_entry.has_outdated_local_hash() and not hash_entry.is_external_task()
        ]

    def get_new_local_hash_entries(self):
        """
        Tasks that are non existent on local machine

        Returns:
            list: hash entries that are not present in local luisy.hash

        """
        return [hash_entry for hash_entry in self.hash_entries if not hash_entry.in_local()]

    def get_downloadable_hash_entries(self):
        """
        Identifies which of the files available in the cloud have the hash needed for the local
        execution.

        Returns:
            list: List of hash entries that can be downloaded from the cloud having the latest hash

        """
        return [hash_entry for hash_entry in self.hash_entries
                if hash_entry.is_downloadable()]

    def get_new_cloud_hash_entries(self):
        """
        Non existent tasks in the cloud

        Returns:
            list: hash entries that do not exist in cloud yet

        """
        return [hash_entry for hash_entry in self.hash_entries
                if not hash_entry.in_cloud() and not hash_entry.has_parquet_output()]

    def get_outdated_cloud_hash_entries(self):
        """
        Outdated tasks in cloud

        Returns:
            list: hash entries of outdated tasks in cloud

        """
        return [hash_entry for hash_entry in self.hash_entries
                if hash_entry.has_outdated_cloud_hash() and not hash_entry.has_parquet_output()]

    def get_all_hash_entries_that_need_a_run(self):
        """
        Outdated or non existing local tasks

        Returns:
            list: Hash entries of tasks that will be refreshed in this luisy run

        """
        return list(set(self.get_new_local_hash_entries() + self.get_outdated_local_hash_entries()))

    def get_local_execution_hash_entries(self):
        """
        Tasks that need to be executed locally because they cannot be downloaded

        Returns:
            list: hashmappings with tasks that need to be executed locally

        """
        return [hash_entry for hash_entry in self.hash_entries
                if (hash_entry.needs_local_execution())]

    def get_download_hash_entries(self):
        """
       Non existent local tasks that are available with their newest versions in the cloud

        Returns:
            list: hashmappings that will be downloaded

        """
        return [hash_entry for hash_entry in self.hash_entries
                if (hash_entry.needs_to_be_downloaded())]

    def get_upload_hash_entries(self):
        """
        All outdated or non existing tasks in cloud

        Returns:
            list: hash entries to upload

        """
        return self.get_outdated_cloud_hash_entries() + self.get_new_cloud_hash_entries()

    def clear_outdated_tasks(self):
        """
        Clears the output of all tasks that are outdated locally

        Note:
            This can also be implemented for cloud clearing here. Currently the rerun tasks in
            the cloud are just overwritten when saved online

        """
        for hash_entry in self.get_outdated_local_hash_entries():
            hash_entry.task.clean()

    def synchronize_local_hashes(self, failed_tasks):
        """
        This function updates all local hashes to the hashes from the current run. Failed tasks
        can be given as an argument to prevent luisy from setting hashes that have not been
        executed.

        Args:
            failed_tasks (list): Failed tasks in luigi run

        """
        executed_tasks = self.get_new_local_hash_entries() + self.get_outdated_local_hash_entries()
        for hash_entry in executed_tasks:
            if hash_entry.task in failed_tasks:
                logger.warning(f"Task for {hash_entry.filename} failed, will not update hash")
                continue
            outfile = hash_entry.filename
            self.hashmapping_local.update(
                filepath=outfile,
                task_hash=self.hashmapping_new.get_hash(outfile)
            )
            self.hashmapping_local.save()

    def synchronize_cloud_hashes(self, failed_tasks, upload_tasks=True):
        """
        This function updates all online/cloud hashes to the hashes from the current run. Failed
        tasks can be given as an argument to prevent luisy from setting hashes that have not been
        uploaded. In addition, if `upload_tasks` is True, this method uploads all the tasks that
        have outdated hash entries aswell.

        Args:
            failed_tasks (list): Failed tasks in luigi run

        """

        for hash_entry in self.get_upload_hash_entries():
            # Only upload successfully tasks
            if hash_entry.task in failed_tasks:
                logger.warning(f"Task for {hash_entry.filename} failed, will not upload")
                continue

            if upload_tasks:
                hash_entry.task.upload(overwrite=True)
            self.hashmapping_cloud.update(
                filepath=hash_entry.filename,
                task_hash=hash_entry.hash_new,
            )
            # Update the cloud file after each upload to not start all over again in terms
            # of crashes
            self.hashmapping_cloud.save()

    def set_files_to_download(self):
        """
        Sets files that need to be downloaded when luigi is executed. Files that need to be
        downloaded will be set inside the Config() singleton and checked in the
        :py:meth`luisy.Target.exists()` method.

        Note:
            Can this be done dynamically during luigi run?
        """
        Config()._files_to_download = [add_working_dir(hash_entry.filename) for hash_entry in
                                       self.get_download_hash_entries()]


class HashMapping(object):
    """
    Maps generic file identifiers to hashes of the tasks. To work in different working-dirs, the
    file identifiers are the paths to the file with the working-dir removed.

    Args:
        hashes (dict): Dict where the keys are file identifiers and the values are hashes of the
            tasks creating the respective files.
        local (bool): Whether the files of the hashes are located locally or remotely (i.e., the
            cloud).
        project_name (str): Identifier of the project. Typically the string marked by the
            decorator :py:func:`luisy.decorators.project_name`
    """

    def __init__(self, hashes, local=True, project_name=None):
        self.hashes = hashes
        self.local = local
        self.project_name = project_name

    @staticmethod
    def get_local_hash_path(project_name):
        return os.path.join(
            Config().working_dir,
            project_name,
            '.luisy.hash'
        )

    @staticmethod
    def get_cloud_hash_path(project_name):
        return change_working_dir(
            path=HashMapping.get_local_hash_path(project_name),
            dir_current=Config().working_dir,
            dir_new='')

    def get_hash(self, filepath):
        return self.hashes.get(filepath, None)

    @classmethod
    def load_from_local(cls, project_name):
        """
        Loads the mapping from the local :code:`.luisy.hash` file available in the working-dir of
        the project.

        Args:
            project_name (str): Name of the project.

        Returns:
            HashMapping: The mapping of the hashes

        """

        filepath = HashMapping.get_local_hash_path(project_name)

        logger.info(f'Read hashes from {filepath}')
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                hashes = json.load(f)
        else:
            # If no hash-file exists, set up one
            logger.info('No hash-file there, will create one. May have to run some tasks')
            hashes = {}

        return HashMapping(hashes, local=True, project_name=project_name)

    def update(self, filepath=None, task_hash=None):
        self.hashes[filepath] = task_hash

    @classmethod
    def load_from_cloud(cls, project_name):
        """
        Loads the mapping from the cloud version of :code:`.luisy.hash` in the cloud-storage of the
        project.

        Args:
            project_name (str): Name of the project.

        Returns:
            HashMapping: The mapping of the hashes

        """

        fs = Config().fs

        filepath = HashMapping.get_cloud_hash_path(project_name)

        if fs.exists(filepath):
            logger.info('File available. Start download')
            hashes = fs.load_json(filepath)

        else:
            logger.info('No hash file in cloud')
            hashes = {}

        return HashMapping(hashes, local=False, project_name=project_name)

    @staticmethod
    def compute_from_tasks(tasks, project_name):
        """
        Computes the mapping from a dict of tasks.

        Args:
            tasks (dict): Dict where the values are :py:class:`luisy.Task` objects and the keys are
                their file identifiers (i.e., the filepaths without the working-directory).
            project_name (str): Name of the project.

        Returns:
            HashMapping: The mapping of the hashes
        """
        hashes = compute_hashes(
            tasks,
            requirements_path=Config().get_param('requirements_path')
        )
        return HashMapping(hashes, local=True, project_name=project_name)

    def save(self):
        """
        Saves the hashes either to disc (if :code:`local=True`) or to the cloud storage (if
        :code:`local=False`)
        """
        if self.local:
            filepath = HashMapping.get_local_hash_path(self.project_name)
            os.makedirs(filepath.replace(os.path.basename(filepath), "", ), exist_ok=True)
            logger.info(f'Write hashes to {filepath}')
            with open(filepath, 'w+') as f:
                json.dump(self.hashes, f)
        else:
            fs = Config().fs
            logger.info('Upload hashes to cloud')
            filepath = HashMapping.get_cloud_hash_path(self.project_name)
            fs.save_json(filepath, self.hashes)

    def items(self):
        return self.hashes.items()

    def exists(self, filepath):
        """
        Tests whether the output behind filepath exists, either locally or the cloud.
        """

        if self.local:
            filepath_local = add_working_dir(filepath)
            return os.path.exists(filepath_local)
        else:
            fs = Config().fs
            return fs.exists(filepath)

    def clean_up(self):
        """
        Checks if for all existing hashes the outfiles are there. If not, the hashes are removed.
        """

        filepaths = list(self.hashes.keys())
        for filepath in filepaths:
            if not self.exists(filepath):
                logger.info(f"{filepath} not existing any more, clean up")
                self.hashes.pop(filepath)


def get_upstream_tasks(task):
    """
    Computes all upstream tasks of the given tasks by enumerating the computational DAG.

    Args:
        task (luisy.base.Task): A luisy task object

    Returns:
        dict: Dict where the values are the upstream tasks and the keys are the respective output
        files.
    """
    tasks = {}

    for dep in get_all_dependencies(task):
        if isinstance(dep, Task):
            tasks[remove_working_dir(dep.output().path)] = dep
        else:
            logger.warning(
                'None-luisy Tasks in your pipeline detected.'
                'Those tasks may prevent the rerun-functionality to work correctly!'
            )
    return tasks


def compute_hashes(tasks, requirements_path=None):
    """
    Computes the hashes for all tasks

    Args:

        tasks (dict): Dict where the values are :py:class:`luisy.Task` objects and the keys are the
            paths to their output.
        requirements_path (str): Path to the code:`requirement.txt`. Defaults to `None` and will
            be read-off the config.
    Returns:
        dict: Dict where the keys are the outfiles of the tasks handed in and the values are the
        hashes.

    TODO:
        Can this be moved to TaskHashEntry?

    """
    filenames = list(tasks.keys())
    task_list = [tasks[f] for f in filenames]

    tasks_objects = []
    tasks_classes = []

    for task in task_list:
        task_class = type(task)
        if task_class not in tasks_classes:
            tasks_classes.append(task_class)
            tasks_objects.append(task)

    hashes = create_hashes(tasks_classes, requirements_path=requirements_path)
    hash_dict = dict(zip(tasks_classes, hashes))

    return {filename: hash_dict[type(task)] for filename, task in tasks.items()}
