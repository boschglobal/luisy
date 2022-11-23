# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import pickle
import json
import luigi
import pandas as pd
import os
import logging
import re

from luisy.config import (
    Config,
    change_working_dir,
)
from luisy.helpers import get_df_from_parquet_dir

logger = logging.getLogger(__name__)


class LocalTarget(luigi.LocalTarget):
    """

    Args:
        path (str): Path to the file
        working_dir (str): The working dir
        download (bool): Whether the file should be downloaded from the cloud if not available
            locally
    """

    file_ending = None

    def __init__(self, path, **kwargs):
        luigi.LocalTarget.__init__(
            self,
            path=path,
            format=None,
            is_tmp=False
        )
        self.kwargs = kwargs

    @property
    def fs(self):
        return Config().fs

    def is_folder(self):
        """
        Checks if the output is a folder.

        .. note::
            This may has to be improved in future as we (silently) assume that any file has file
            extension.

        """
        _, file_ending = os.path.splitext(os.path.basename(self.path))
        return file_ending == ''

    def make_dir(self, path):
        """
        Creates the local path `path`.

        """
        logger.info(f'Create dir {path}')
        os.makedirs(path, exist_ok=True)

    def remove(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        else:
            logger.info(f'Target {self.path} not existing, nothing to remove')

    def exists(self):
        """
        Checks if the file exists. If :code:`download` is set to :code:`True`, then it is checked
        whether the file is available in the cloud if it is not available locally. If it is
        available, it is downloaded.
        """

        file_exists_locally = os.path.exists(self.path)

        if not file_exists_locally and Config().get_param('download'):
            if self.path in Config()._files_to_download:
                logger.info('Task not complete locally, check if file is available in cloud')
                self._try_to_download()

            # Check again if file exists
            file_exists_locally = os.path.exists(self.path)

        return file_exists_locally

    def _try_to_download(self):
        """
        Tries to download the target file from the cloud
        """
        path = change_working_dir(
            path=self.path,
            dir_current=Config().working_dir,
            dir_new='')

        if self.fs.exists(path):
            logger.info('File available. Start download')

            if self.is_folder():
                logger.info('Download folder')

                self.fs.download(
                    source=path,
                    dest=self.path)
            else:
                logger.info('Download file')
                self.fs._download_file(
                    source=path,
                    dest=self.path,
                )
        else:
            logger.info('File not in cloud.')

    def _try_to_upload(self, overwrite=False):
        """
        Tries to upload the target file to the cloud
        """
        path = change_working_dir(
            path=self.path,
            dir_current=Config().working_dir,
            dir_new='')

        if self.is_folder():
            raise NotImplementedError('Not implemented yet')

        logger.info('File not available. Start upload')
        self.fs._upload_file(
            source=self.path,
            dest=path,
            overwrite=overwrite,
        )


class PickleTarget(LocalTarget):
    file_ending = 'pkl'

    def write(self, obj):
        with open(self.path, 'wb') as f:
            pickle.dump(obj, f)

    def read(self):
        with open(self.path, 'rb') as f:
            return pickle.load(f)


class HDFTarget(LocalTarget):
    file_ending = 'hdf'

    def write(self, df):
        df.to_hdf(self.path, key='data')

    def read(self):
        return pd.read_hdf(self.path)


class XLSXTarget(LocalTarget):
    file_ending = 'xlsx'

    def write(self, df):
        df.to_excel(self.path)

    def read(self):
        return pd.read_excel(self.path, **self.kwargs)


class CSVTarget(LocalTarget):
    file_ending = 'csv'

    def write(self, df):
        df.to_csv(self.path, index=False, **self.kwargs)

    def read(self):
        return pd.read_csv(self.path, **self.kwargs)


class ParquetDirTarget(LocalTarget):
    file_ending = ''

    def write(self, df):
        raise NotImplementedError("Coming soon...")

    def read(self):
        return get_df_from_parquet_dir(self.path)


class JSONTarget(LocalTarget):
    file_ending = 'json'

    def write(self, dct):
        assert isinstance(dct, dict)
        with open(self.path, 'w') as f:
            json.dump(dct, f, indent=4, separators=(',', ': '))

    def read(self):
        with open(self.path, 'r') as f:
            return json.load(f)


class DirectoryTarget(LocalTarget):
    """
    Target to read directories. The read_file method has to be filled by the user and passed to
    target by decorating it.
    """
    file_ending = ''
    regex_pattern = None

    def __init__(self, path, **kwargs):
        luigi.LocalTarget.__init__(
            self,
            path=path,
            format=None,
            is_tmp=False
        )
        if self.regex_pattern is None:
            self.regex_pattern = ''

        self.kwargs = kwargs

    def write(self):
        raise NotImplementedError("Coming soon...")

    def is_valid_file(self, filename):
        """
        Checks if file matches given regex

        Args:
            filename (str): file that need to be checked against regex

        Returns:
            bool: flag if file matches regex

        """
        return re.search(self.regex_pattern, filename)

    def read(self):
        for i, filename in enumerate(os.listdir(self.path)):
            if self.is_valid_file(filename):
                content = self.read_file(os.path.join(self.path, filename))
                if content is not None:
                    yield content

    def read_file(self, file):
        raise NotImplementedError('Needs to be passed by user. See '
                                  'decorators.make_directory_output')
