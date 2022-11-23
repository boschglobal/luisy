# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0


import luigi
import logging
import pandas as pd
import os
import traceback

from luisy.decorators import (
    pickle_output,
    auto_filename,
)
from luisy.config import Config
from luisy.visualize import visualize_task

from luisy.helpers import RegexTaskPattern

logger = logging.getLogger(__name__)


@pickle_output
@auto_filename
class Task(luigi.Task):
    """
    Base task that provides interfaces for all decorators.
    """

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(name=self.__class__.__name__)

        # May  be set via decorator
        if not hasattr(self, 'project_name'):
            self.project_name = self.__class__.__module__.split('.')[0]

        if self.project_name == 'luisy':
            raise ValueError(
                'Something went wrong when setting the project name.'
                'Are you using the task correctly?'
            )

        super().__init__(*args, **kwargs)

    @classmethod
    def get_related_instances(cls, regex_placeholder='[\\S]*'):
        """
        Find executed instances of the task class on your local machine.

        Args:
            regex_placeholder (str): Regex placeholder that replaces value strings with a regex to
                find all matching instances on filesystem.

        Returns:
            list[luisy.Task]: list of task instances related to given class

        Raises:
            Exception: If problems in finding instances on the system occur, Exceptions are
                caught in general and the user is informed to look at his get_file_name() method.

        """
        try:
            regex_pattern = RegexTaskPattern(
                task_cls=cls,
                regex_placeholder=regex_placeholder
            )
            params = regex_pattern.get_available_params()
            return [cls(**entry) for entry in params.values()]
        except Exception:
            logger.info('Finding instances failed, have you overwritten Task.get_file_name() '
                        'method of your class?')
            logger.error(traceback.format_exc())
            raise

    def get_outdir(self):
        out_dir = Config().get_param('working_dir')
        out_dir = os.path.join(
            out_dir, self.get_sub_dir())

        return out_dir

    def get_file_name(self):
        """
        By overriding this method one can specify a custom filename.
        """
        pass

    def get_sub_dir(self):
        """
        Default implementation of a subdir.
        By overriding this method one can specify an additional subdir
        under the current working dir.
        """
        return ""

    def get_outfile(self):
        out_dir = self.get_outdir()
        os.makedirs(out_dir, exist_ok=True)
        file_name = self._remove_redundant_file_ending()
        return os.path.join(
            out_dir,
            f"{file_name}.{self.target_cls.file_ending}"
        )

    def _remove_redundant_file_ending(self):
        file_extension = "." + self.target_cls.file_ending
        if self.get_file_name().endswith(file_extension):
            return self.get_file_name()[:-len(file_extension)]
        else:
            return self.get_file_name()

    def download(self):
        """
        Downloads the output of the task from the cloud.
        """
        self.logger.info(f'Start downloading of {self}')
        self.output()._try_to_download()

    def upload(self, overwrite=False):
        """
        Uploads the output of the task to the cloud.abs

        Args:
            overwrite (bool): Whether an existing file in the cloud should be overwritten. Defaults
                to :code:`False`.
        """
        self.logger.info(f'Start uploading of {self}')
        self.output()._try_to_upload(overwrite=overwrite)

    def read(self):
        """
        Reads the output of the task and return is. If cloud synchronisation is activated, a file
        may be downloaded from the cloud
        """

        if not self.complete():
            raise ValueError('Task needs to be executed first, '
                             'try luisy.helpers.get_related_instances() to find executed '
                             'instances of this task')

        return self.output().read()

    def write(self, obj):
        self.logger.info('Write to outfile')
        target = self.output()
        target.make_dir(self.get_outdir())
        target.write(obj)

    def output(self):
        """
        """
        return self.target_cls(
            path=self.get_outfile(),
            **self.target_kwargs)

    def clean(self):
        if isinstance(self, luigi.ExternalTask):
            raise ValueError('External tasks cannot be deleted that way')
        self.logger.info(f"Cleaning {self}")
        self.output().remove()

    def visualize(self, ax=None, unique_children=True, parameters_to_exclude=()):
        """
        Visualizes the dependencies tree of the task.

        Args:
            ax (matplotlib.axis.Axis): A matplotlib axis to draw in
            unique_children (bool): If true, add only subset of unique types of children
            parameters_to_exclude (list): List of names of py:mod:`luigi`-parameters to exclude in
                visualization.

        """
        visualize_task(
            task=self,
            ax=ax,
            unique_children=unique_children,
            parameters_to_exclude=parameters_to_exclude,
        )


class ExternalTask(Task, luigi.ExternalTask):
    pass


class ConcatenationTask(Task):
    def preprocess(self, df):
        return df

    def run(self):
        """
        """

        dfs = []
        for infile in self.input():
            self.logger.info('Read file {}'.format(infile.path))
            dfs.append(infile.read())

        df = pd.concat(dfs)
        df = self.preprocess(df)
        self.write(df)


class WrapperTask(Task, luigi.WrapperTask):
    def download(self):
        """
        Task cannot be downloaded, because no file exists.
        """
        pass

    def upload(self, overwrite=None):
        """
        Task cannot be uploaded, because no file exists.
        """
        pass

    def read(self):
        """
        Reads all the output of the required wrapped tasks. The type of the return value depends on
        the type of the return value of the :py:func:`WrapperTask.requires` method.
        """
        input_tasks = self.input()

        if isinstance(input_tasks, dict):
            return {
                k: task.read() for k, task in input_tasks.items()
            }

        if isinstance(input_tasks, list):
            return [task.read() for task in input_tasks]

        raise ValueError('Type of requires not understood')
