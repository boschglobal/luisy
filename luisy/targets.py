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
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.errors.exceptions.connect import AnalysisException
from luisy.config import (
    Config,
    change_working_dir,
)
from luisy.helpers import get_df_from_parquet_dir

logger = logging.getLogger(__name__)


class LuisyTarget(luigi.LocalTarget):
    """

    Args:
        path (str): Path to the file
        working_dir (str): The working dir
        download (bool): Whether the file should be downloaded from the cloud if not available
            locally
    """

    @property
    def fs(self):
        return Config().fs

    def exists(self):
        raise NotImplementedError()

    def write(self, obj):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()

    def remove(self):
        raise NotImplementedError()

    # TODO: Should this be a public method?
    def _try_to_upload(self, overwrite=False):
        raise NotImplementedError()

    # TODO: Should this be a public method?
    def _try_to_download(self):
        raise NotImplementedError()


class LocalTarget(LuisyTarget):
    file_ending = None

    def __init__(self, path, **kwargs):
        luigi.LocalTarget.__init__(
            self,
            path=path,
            format=None,
            is_tmp=False
        )
        self.kwargs = kwargs

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


class CloudTarget(LuisyTarget):

    def __init__(self, path, **kwargs):
        LuisyTarget.__init__(
            self,
            path=path,
            format=None,
            is_tmp=False
        )
        self.kwargs = kwargs


class DeltaTableTarget(CloudTarget):
    # TODO: Can we get rid of fileending
    file_ending = 'DeltaTable'

    def __init__(
            self,
            outdir=None,
            schema="schema",
            catalog="catalog",
            table_name=None
    ):
        self.outdir = outdir
        self.table_name = table_name
        self.schema = schema
        self.catalog = catalog

    @property
    def spark(self):
        try:
            return Config().spark
        except AttributeError:
            raise AttributeError(
                "spark session was not found in config. Make sure to have all the databricks "
                "parameters set in order to start the spark session!"
            )

    def make_dir(self, path):
        # TODO: Nothing to do here, adapt interface?
        pass

    def remove(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_uri}")

    @property
    def table_uri(self):
        return f"{self.catalog}.{self.schema}.{self.table_name}"

    @property
    def path(self):
        # TODO: Path here is more an identifier that shows up in `.luisy.hashes`
        return os.path.join(
            self.outdir,
            f"{self.table_uri}.{self.file_ending}",
        )

    def exists(self):
        """
        Checks whether the Deltatable exists.

        Note:
            Ideally, we would call `self.spark.catalog.tableExists(self.table_uri)` to check whether
            the table exists, but this always returns `False`.
        """
        try:
            self.spark.sql(f"SELECT 1 from {self.table_uri}")
            return True
        except AnalysisException:
            return False

    def write(self, df: SparkDataFrame):
        """

        Args:
            df (pyspark.sql.DataFrame): Dataframe that should be written to delta table
        """
        logger.info(f"Drop table {self.table_uri}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_uri}")
        logger.info(f"Write to {self.table_uri}")
        df.write.saveAsTable(self.table_uri)

    def read(self):
        return self.spark.table(self.table_uri)


class AzureBlobStorageTarget(CloudTarget):
    file_ending = "pkl"

    def __init__(
            self,
            outdir=None,
            blob_name=None,
    ):
        self.outdir = outdir
        self.blob_name = blob_name

    @property
    def blob_client(self):
        return self.fs.get_blob_client(
            blob=self.path,
        )

    def make_dir(self, path):
        pass

    def remove(self):
        if self.exists():
            self.blob_client.delete_blob()

    @property
    def path(self):
        # TODO: Path here is more an identifier that shows up in `.luisy.hashes`
        return os.path.join(
            self.outdir,
            f"{self.fs.container_name}.{self.blob_name}.{self.file_ending}"
        )

    def exists(self):
        """
        Checks whether the Azure Blob exists.

        """

        return self.fs.exists(self.blob_name)

    def write(self, obj):
        """
        Write object to Azure Blob Storage
        Args:
            obj (object): object that can be serialized using `pickle` and is to be written to
                the blob output
        """

        try:
            serialized_obj = pickle.dumps(obj)
        except:
            return TypeError("Object could not be serialized. Currently, only objects that can be "
                             "pickled can be stored in Azure Blob Storage.")
        self.blob_client.upload_blob(serialized_obj, overwrite=True)

    def read(self):
        """
        Read object from Azure Blob
        """

        # is it a problem, that we use two different blob clients for exists and download?
        if not self.exists():
            raise ValueError(
                f"Blob '{self.blob_name}' does not exist.")

        blob_data = self.blob_client.download_blob()
        return pickle.loads(blob_data.readall())


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
