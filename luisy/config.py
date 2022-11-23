# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0
"""
This module contains all the management of luisy's configuration. It holds a singleton, that can
be used anywhere in the project after it was initialized in :py:mod:`luisy.__init__`. Also the
cli parameters are passes into this config file in py:func:`luisy.cli.luisy_run()`.
This singleton allows us to access parameters like `working_dir`, `download`, ... anywhere in our
pipelines. We don't need to pass arguments through that pipeline anymore to get the information
into the leafs of our DAG.
"""

import os
import logging
from luisy.file_system import AzureContainer
from luisy.default_params import (
    default_params,
    env_keys
)

logger = logging.getLogger('luigi-interface').getChild('luisy-interface')


def get_default_params(raw=True):
    if raw:
        return default_params.copy()
    else:
        return {key: val for key, val in default_params.items() if val is not None}


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Config(metaclass=Singleton):

    def __init__(self):
        self._config = get_default_params()

        self._files_to_download = []

        for param, env_key in env_keys.items():
            self.set_param(param, self._get_env_var(env_key))

        if self.get_param('azure_storage_key') is not None:
            self.fs = AzureContainer(
                account_name=self.get_param('azure_account_name'),
                container_name=self.get_param('azure_container_name'),
                key=self.get_param('azure_storage_key')
            )

    def update(self, params):
        """
        Takes dict and updates the config with all entries in that dict

        Args:
            params (dict): new params that should be set
        """
        self._config.update(params)

    def set_param(self, name, val):
        """
        Set param in config.

        Args:
            name (str): Key to parameter
            val (object): Value of parameter
        """
        self._config[name] = val

    def get_param(self, param):
        """
        Get param from config.

        Args:
            param (str): Key to parameter

        Returns:
            Value of parameter
        """
        if param not in self._config:
            raise ValueError(f'{param} not found in Config. Please add to default params')
        return self._config[param]

    @property
    def download(self):
        return self.get_param('download')

    @property
    def upload(self):
        return self.get_param('upload')

    @property
    def working_dir(self):
        return os.path.normpath(self.get_param('working_dir'))

    @property
    def config(self):
        """
        Get the whole config

        Returns:
            dict: Config with all parameter values set right now
        """
        return self._config

    def _get_env_var(self, param):
        """
        Tries to get given param out of the environment variables. The logger throws a warning if
        the variable cannot be found in the system

        Args:
            param(str): key of environ variable e.g. `WORKING_DIR`

        Returns:
            str or None: Value of environ variable
        """
        if param not in os.environ:
            logger.warning(f'Environment Variable {param} not set!')
            return None
        return os.environ[param]

    def reset(self):
        """
        Resets the config singleton to the initial state with default parameters and environment
        variable values.
        """
        self._config = None
        self.__init__()

    def check_params(self):
        """
        Way to check if parameters are valid and luisy is ready to initiate the luigi run.
        Currently `working_dir` has to be set and also if the user wants to use the cloud, luisy
        only allows runs when `azure_storage_key`, `azure_account_name`, and `azure_container_name`
        are set.

        Raises:
            ValueError: When parameters are wrong or missing

        """
        needs_azure = self._config['download'] or self._config['upload']

        for azure_param in ['azure_storage_key', 'azure_container_name', 'azure_account_name']:

            if (self._config[azure_param] is None) and needs_azure:
                raise ValueError(
                    f"Parameter '{azure_param}' not set. You cant use download "
                    "or upload functionality without setting your Azure storage key. More "
                    "information: docs/cloud.rst"
                )
        if self._config['working_dir'] is None:
            raise ValueError("Parameter 'working_dir' not set!")


def pass_args(args):
    Config().update(args)


def activate_download():
    Config().set_param('download', True)


def set_working_dir(working_dir):
    Config().set_param('working_dir', working_dir)


def remove_working_dir(path):
    """
    Removes the working dir from the filepath.

    Example:

        If :code:`path=/data/my_project/raw/some_file.pkl`, then the output is
        :code:`/my_project/raw/some_file.pkl`

    Args:

        path (str): Path to a file

    Returns:
        str: Path to the file where working dir has been removed.
    """
    return change_working_dir(
        path=path,
        dir_current=Config().working_dir,
        dir_new='')


def add_working_dir(path):
    """
    Adds the working dir to a filepath.

    Example:

        If the input is :code:`path=/my_project/raw/some_file.pkl` and the working dir is
        :code:`/mnt/d/data`, then the output is :code:`/mnt/d/data/my_project/raw/some_file.pkl`.

    Args:

        path (str): Path to a file without working dir

    Returns:
        str: Path to the file with working dir prepended.
    """

    return os.path.join(Config().working_dir, path.lstrip('/'))


def change_working_dir(path, dir_current, dir_new):
    """
    Exchanges the working dir in :code:`path`

    Args:
        path (str): Path where working dir should be changed
        dir_current (str): The path of the working dir to be replaced
        dir_new (str): The working dir that should be inserted

    Returns:
        str: Path with updated working dir
    """

    # Make sure they do not end with slash
    dir_current = os.path.normpath(dir_current)
    assert path[:len(dir_current)] == dir_current

    if len(dir_new) > 0:
        dir_new = os.path.normpath(dir_new)
        return os.path.join(
            dir_new, path[len(dir_current):].lstrip('/')
        )
    else:
        return path[len(dir_current):]
