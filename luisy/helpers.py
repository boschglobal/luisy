# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import os
import re
from functools import reduce
from luigi.date_interval import Week
from luigi.tools.deps import get_task_requires
import copy
from contextlib import contextmanager
import glob
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_string_repr(obj):
    """
    Computes a string representation of the given object.

    Args:
        obj (object): An arbitrary python object

    Returns:
        str: A string representation
    """

    if hasattr(obj, '__iter__') and type(obj) != str:
        return "_".join([get_string_repr(o) for o in obj])
    else:
        return str(obj).replace(' ', '_').replace('/', '_')


def get_start_date(week):
    """
    Extracts the first day of the week.

    Args:
        week (luigi.date_interval.Week or string): Week that is compatible with luigi or string of
            the form :code:`2021-W12`

    Returns:
        datetime.date: The first day of the week
    """
    logger.info('passed in week is {}'.format(week))
    if isinstance(week, Week):
        logger.info('returning date is {}'.format(week.date_a))
        return week.date_a
    else:
        logger.info('returning date is {}'.format(Week.parse(week).date_a))
        return Week.parse(week).date_a


def get_stop_date(week):
    """
    Extracts the last day of the week.

    Args:
        week (luigi.date_interval.Week or string): Week that is compatible with luigi or string of
            the form :code:`2021-W12`

    Returns:
        datetime.date: The last day of the week
    """
    logger.info('passed in week is {}'.format(week))
    if isinstance(week, Week):
        logger.info('returning date is {}'.format(week.date_b))
        return week.date_b
    else:
        logger.info('returning date is {}'.format(Week.parse(week).date_b))
        return Week.parse(week).date_b


def get_all_reqs(task, visited=None):
    if visited is None:
        visited = [task]

    for next in get_task_requires(task) - set(visited):
        visited += get_all_reqs(next, visited=[next])

    return list(set(visited))


def add_filter_info(filter_func):
    """
    this decorator for class methods adds information about the change of a pd.DataFrame's shape
    before (and after if function returns a pd.DataFrame) filtering the df to the logger.
    This provides additional information to the user which function filtered how many rows / cols.
    This can be easily applied to every task method like this:

    .. code-block:: python

        @add_filter_info
        def filter_func(df):
            return df.drop(["my_most_disliked_column"], axis=1)

    Args:
        filter_func (cls.method): method that takes pd.DataFrame as first argument and filters it by
            some operations

    Returns:
        wrapper around filter_func, that adds additional information about filtered pd.DataFrame.

    """

    def wrapper(cls, df, *args, **kwargs):
        logger.info(f"{cls.__class__}:")
        logger.info(f"df.shape before '{filter_func.__name__}': {df.shape}")
        df = filter_func(cls, df, *args, **kwargs)
        if df is not None:
            logger.info(f"df.shape after '{filter_func.__name__}': {df.shape}")
            return df

    return wrapper


def wrap_in_brackets(string):
    return '(' + string + ')'


class RegexParam:
    """
    Class to hold information about a Parameter which RegexTaskPattern class uses to create
    matching regex queries.

    Args:
        name (str): Parameter name in luisy.Task
        default (object): parameters original default value
        regex_placeholder (str): regex string which matches all possible parameter values.
    """

    def __init__(self, name, default, regex_placeholder='[\\S]*'):
        self.name = name
        self.default = default
        self.regex_placeholder = regex_placeholder

    @property
    def placeholder(self):
        """
        This creates a unique placeholder, which is used for finding the location of the
        parameter inside a string. i.e. filepath.

        Returns:
            str: parameter surrounded with "*"
        """
        return f'*{self.name}*'


class RegexTaskPattern:
    """
    This class can be used to find instances of a task on the disk. It uses regex to search for
    in the user's filesystem. Each param is then replaced with a placeholder to build the regex
    from it.

    Args:
        task_cls (Type[luisy.Task]): a class object of type luisy.Task. The given task should use
            the :py:func:`luisy.decorator.auto_filename` to work properly.
        regex_placeholder (str): This is the regex which is inserted into the filename to create
            a matching  regex, which then searches the root directory.

    Note:
        Only works when using @auto_filename
    """

    def __init__(self, task_cls, regex_placeholder='[\\S]*'):
        self.task_cls = task_cls
        self.params = [
            RegexParam(
                name=param_name,
                default=copy.deepcopy(getattr(task_cls, param_name)._default),
                regex_placeholder=regex_placeholder
            ) for param_name in task_cls.get_param_names()
        ]
        self.regex_placeholder = regex_placeholder
        self.set_filename_pattern()

        self.regex_pattern = self.build_regex()
        self.root_dir = self.build_root_directory()

    @contextmanager
    def _overwrite_defaults(self):
        """
        This method works as a contextmanager to overwrite a task classes default params. This
        is used to extract filename strings, which can be used to build regex queries.

        Yields:
            Overwritten class where the parameter defaults are overwritten with placeholders

        """
        for param in self.params:
            getattr(self.task_cls, param.name)._default = param.placeholder
        yield self.task_cls
        for param in self.params:
            getattr(self.task_cls, param.name)._default = param.default

    def set_filename_pattern(self):
        """
        This creates a filename pattern with parameter values filled by
        :py:func:`RegexParam.placeholder()`
        """
        with self._overwrite_defaults() as task_cls:
            task = task_cls()
            self.filename_pattern = task.output().path

    def build_root_directory(self):
        """
        Finds "highest" possible path that can be used as a root directory.

        Returns:
            str: Root directory where the regex search is applied on the files inside it.

        """
        path, file = os.path.split(self.filename_pattern)
        dirs = path.split(os.sep)
        smallest_idx = None
        for param in self.params:
            if param.placeholder in dirs:
                smallest_idx = dirs.index(param.placeholder)
        if smallest_idx is not None:
            return os.sep.join(dirs[:smallest_idx])
        return path

    def build_regex(self):
        """
        Build a regex string from the filepath. This replaces the parameter values of the task
        with a regex pattern that matches these values.

        Returns:
            str: Regex pattern that can be used to search for files of that type

        """
        logger.info('Building Regex')
        regex_pattern = self.filename_pattern
        for param in self.params:
            regex_pattern = regex_pattern.replace(param.placeholder, param.regex_placeholder)
        path = os.path.normpath(regex_pattern)
        file_regex = wrap_in_brackets(self.regex_placeholder).join(
            [wrap_in_brackets(group) for group in path.split(
                self.regex_placeholder)]
        )

        return file_regex

    def build_re_splitters(self):
        """
        The splitters are needed to extract the parameters of a file that matches the regex.

        Returns:
            dict: keys are parameter names, values is a list of splits of the filesname. This
                allows us to separate the value from the filename

        """
        splits = {}
        for param in self.params:
            repls = [(p.placeholder, p.regex_placeholder) for p in self.params
                     if p.name != param.name]
            replaced_str = reduce(lambda a, kv: a.replace(*kv), repls, self.filename_pattern)
            splits[param.name] = replaced_str.split(param.placeholder)
        return splits

    def get_available_files(self):
        """
        Scans all files in the classes' root_directory and then filters on the regex_pattern of
        this class.

        Returns:
            list: List of files which match the regexpattern and are in the root directory

        """
        file_paths = []
        logger.info(f'Looking for matching regex in {self.root_dir}')
        for root, dirs, files in os.walk(self.root_dir):
            for file in files:
                file_paths.append(os.path.join(root, file))
        r = re.compile(self.regex_pattern)
        files = list(filter(r.match, file_paths))
        logger.info('Found matching files:')
        logger.info(files)
        return files

    def get_available_params(self):
        """
        Searches for Files in the rootdirectory matching to the regex and extracts the parameters
        out of the filesnames using the regex_splits.

        Returns:
            dict: keys are file_paths of the files found, values are a dict with param_name as
                key and param_value as value.

        """
        values = {}
        files = self.get_available_files()
        re_splitters = self.build_re_splitters()
        logger.info('Extract params from files')
        for file_path in files:
            file_params = {}
            for param in self.params:
                prefix_splitter = re_splitters[param.name][0]
                suffix_splitter = re_splitters[param.name][1]
                truncated_prefix = re.split(prefix_splitter, file_path)[1]
                file_params[param.name] = re.split(suffix_splitter, truncated_prefix)[0]
            values[file_path] = file_params
        logger.info('Params extracted:')
        logger.info(list(values.values()))
        return values


def get_related_instances(task_cls, regex_placeholder='[\\S]*'):
    regex_pattern = RegexTaskPattern(
        task_cls=task_cls,
        regex_placeholder=regex_placeholder
    )
    params = regex_pattern.get_available_params()
    return [task_cls(**entry) for entry in params.values()]


def get_df_from_parquet_dir(path):
    """
    Reads all Parquet files from a given directory and parses them into a pandas dataframe

    Args:
        path (str): path where parquet files are stored

    Returns:
        pd.DataFrame: dataframe with all the data from parquet files
    """

    files = glob.glob(os.path.join(path, '*.parquet'))
    dfs = [pd.read_parquet(f, engine='pyarrow') for f in files]

    # Remove all data frames with no lines
    dfs = filter(lambda df: df.shape[0] > 0, dfs)

    try:
        df = pd.concat(dfs)
        df.reset_index(inplace=True, drop=True)
    except ValueError:
        raise ValueError(f"Could not find any parquet files in '{path}'")

    return df


def remove_parent_directories(lst_with_substring_elements):
    """
    Removes elements from list that are substrings from other items in that list. E.g.
    ['/mnt/d', '/mnt/d/a', '/mnt/d/b'] removes '/mnt/d' from the list.
    Args:
        lst_with_substring_elements(list): removes substrings from that list

    Returns:
        List: Clean list without substrings of other elements, sorted by character length
    """
    lst_with_substring_elements.sort(key=lambda s: len(s), reverse=True)
    clean_lst = []
    for element in lst_with_substring_elements:
        if not any([element in file for file in clean_lst]):
            clean_lst.append(element)
    return clean_lst
