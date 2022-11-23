# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0
"""
This module holds various decorators to be attached to :py:class:`~luisy.tasks.base.Task` objects.
"""
import os
from luisy.config import Config
from luisy.targets import (
    HDFTarget,
    PickleTarget,
    XLSXTarget,
    CSVTarget,
    ParquetDirTarget,
    JSONTarget,
    DirectoryTarget,
)
from luigi.util import inherits
from luigi import ExternalTask
from luisy.helpers import get_string_repr


class requires(object):
    """
    A backwards compatible extension to :py:class:`luigi.util.requires` which allows to access the
    requirements in a dict like fashion.
    """

    def __init__(self, *tasks_to_require, as_dict=False):
        if not tasks_to_require:
            raise TypeError("tasks_to_require cannot be empty")
        self.tasks_to_require = tasks_to_require
        self.as_dict = as_dict

    def __call__(self, task_that_requires):
        task_that_requires = inherits(*self.tasks_to_require)(task_that_requires)

        def _requires(_self):
            """
            """
            if len(self.tasks_to_require) == 1:
                return _self.clone_parent()
            else:
                if self.as_dict:

                    return {
                        t.get_task_family(): _self.clone(t) for t in self.tasks_to_require
                    }

                else:
                    return _self.clone_parents()

        task_that_requires.requires = _requires
        return task_that_requires


def _make_output_decorator_factory(target_cls):
    def output_decorator_factory(cls_py=None, **kwargs):
        def output_decorator(cls):
            cls.target_cls = target_cls
            cls.target_kwargs = kwargs
            return cls
        return output_decorator(cls_py) if callable(cls_py) else output_decorator
    return output_decorator_factory


hdf_output = _make_output_decorator_factory(HDFTarget)
pickle_output = _make_output_decorator_factory(PickleTarget)
xlsx_output = _make_output_decorator_factory(XLSXTarget)
csv_output = _make_output_decorator_factory(CSVTarget)
json_output = _make_output_decorator_factory(JSONTarget)


def parquetdir_output(cls_py=None):
    def decorator(cls):
        """
        """
        cls.target_cls = ParquetDirTarget

        if not issubclass(cls, ExternalTask):
            raise ValueError(
                "This decorator only works on ExternalTasks"
            )

        if not hasattr(cls, 'get_folder_name'):
            raise ValueError(
                "When using ParquetDirTarget, a method get_folder_name()"
                "needs to be implemented in your luisy task"
            )

        def get_outfile(self):
            return os.path.join(
                self.get_outdir(),
                self.get_folder_name()
            )

        cls.get_outfile = get_outfile
        return cls

    return decorator(cls_py) if callable(cls_py) else decorator


def make_directory_output(read_file_func, regex_pattern=None):
    """
    This function is used to give the user the ability to create his own directory outputs for
    external Tasks. Decorating a :py:class:ExternalTask with this function allows the user to use
    :py:meth:`luisy.ExternalTask.output().read` in the task that requires the external task. The
    files will be passed one by one as a generator when calling task.input().read().

    Args:
        read_file_func: function read_file(file), where the user can define how his data is being
            loaded. return the filecontent in this function. To skip specific incoming files, make
            sure that your `read_file_func` function returns None.
        regex_pattern (str): regex string that can be passed. All files found in the directory
          will be checked against this regex and only be parsed if it matches.

    Returns:
        func: Decorator that can be attached to :py:class:`ExternalTask`
    """

    def directory_output(cls):
        if not issubclass(cls, ExternalTask):
            raise ValueError(
                "This decorator only works on ExternalTasks"
            )
        if not hasattr(cls, 'get_folder_name'):
            raise ValueError(
                "When using the directory_output decorator, a method get_folder_name()"
                "needs to be implemented in your luisy task"
            )

        cls.target_cls = DirectoryTarget

        def read_file(self, file):
            return read_file_func(file)

        def get_outfile(self):
            return os.path.join(
                self.get_outdir(),
                self.get_folder_name()
            )

        cls.target_cls.read_file = read_file
        cls.target_cls.regex_pattern = regex_pattern
        cls.get_outfile = get_outfile
        return cls

    return directory_output


def project_name(project_name):
    """
    Sets the project_name of the task to `project_name` to overwrite the default behavior of
    :py:class:`luisy.Task`

    Args:
        project_name (str): Name of the project
    """

    def decorator(cls):
        cls.project_name = project_name
        return cls

    return decorator


def _make_dir_layer(layer_name):
    def dir_layer(cls_py=None):
        def decorator(cls):
            def get_outdir(self):
                out_dir = os.path.join(
                    Config().get_param('working_dir'), self.project_name, layer_name)
                out_dir = os.path.join(
                    out_dir, self.get_sub_dir())
                return out_dir

            cls.get_outdir = get_outdir
            return cls
        return decorator(cls_py) if callable(cls_py) else decorator
    return dir_layer


def auto_filename(cls_py=None, excl_params=None, excl_none=False):
    """
    "Factory" for a Decorator which automatically generates the basename of the outfile based on
    the name of the class and the :py:class:`luigi.Parameter` objects attached to it. Parameters
    allow the user to exclude some parameters and also to exclude None values in general

    Args:
        cls_py (class): class that gets decorated. This parameter is kind of a hack to allow
            usage of this decorator without parenthesis "()". So the end-user is able to call
            `@auto_filename` (important for backwards compatibility),
            `@auto_filename(excl_list=['x', 'y'])` and `@auto_filename()` as well with all having
            the class below decorated
        excl_params (list): List of task parameters that should be excluded
        excl_none (bool): If True, None values are not part of the filename

    Returns:
        class: Decorated class

    """
    assert callable(cls_py) or cls_py is None
    if excl_params is None:
        excl_params = []

    def _decorator(cls):
        def get_file_name(self):
            filename = self.get_task_family()
            for p in sorted(self.get_param_names()):
                if p not in excl_params and p != 'working_dir':
                    if excl_none and getattr(self, p) is None:
                        continue
                    param_value = get_string_repr(getattr(self, p))
                    filename += f'_{p}={param_value}'
            return filename

        cls.get_file_name = get_file_name
        return cls

    return _decorator(cls_py) if callable(cls_py) else _decorator


interim = _make_dir_layer('interim')
final = _make_dir_layer('final')
raw = _make_dir_layer('raw')
