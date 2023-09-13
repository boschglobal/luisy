# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import os
from azure.storage.blob import BlobServiceClient
import tempfile
import json
import logging
import shutil
from luisy.helpers import (
    get_df_from_parquet_dir,
    remove_parent_directories,
)
logger = logging.getLogger(__name__)


class AzureContainer(object):
    """
    Abstraction for an Azure container.

    Args:
        account_name (str): Account name of the azure storage
        container_name (str): Account name of the azure container
        key (str): Secret key of the container within the storage account
    """

    def __init__(self, account_name="", key=None, container_name=""):

        if key is None:
            raise ValueError('Key needs to be given')

        os.environ["AZURE_CLI_DISABLE_CONNECTION_VERIFICATION"] = "1"
        os.environ["ADAL_PYTHON_SSL_NO_VERIFY"] = "1"

        client = BlobServiceClient(
            account_url="https://{}.blob.core.windows.net".format(account_name),
            credential=key)
        self.client = client.get_container_client(container_name)

    def exists(self, path):
        """
        Checks if dir or file exists.

        Args:
            path (string): Path within the container, like :code:`/path/to/folder`

        Returns:
            bool: True, if file / folder exists. False otherwise.
        """
        path = self._adapt_remote_path(path)

        if path.endswith('/'):
            path = path[:-1]

        for blob in self.client.list_blobs(name_starts_with=path):
            # Either path is a directory, in this case the remote blob directory needs one
            # containing file and the path is then a substring of the blob name
            if blob['name'].startswith(path + '/'):
                return True

            # If the path is a file, then must be a blob with the exact same name
            if path == blob['name']:
                return True
        return False

    def _adapt_remote_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return path

    def download(self, source, dest):
        """
        Downloads the content of the remote directory to the local directory.

        Args:
            source (str): Path to the directory within the container, like :code:`/path/to/data`
            dest (str): Path to local directory.

        Raises:
            ValueError: If local folder exists already
        """
        source = self._adapt_remote_path(source)
        if not dest:
            raise Exception('A destination must be provided')

        tmpdir = tempfile.mkdtemp()

        if os.path.exists(dest):
            raise ValueError('Local dir {} exists already'.format(dest))

        if not source.endswith('/'):
            source += '/'
        if not dest.endswith('/'):
            dest += '/'

        blobs = self._ls_files(source, recursive=True)
        file_blobs = remove_parent_directories(lst_with_substring_elements=blobs)
        file_blobs = [source + blob for blob in file_blobs]

        for blob in file_blobs:
            blob_dest = os.path.join(tmpdir, os.path.relpath(blob, source))
            self._download_file(blob, blob_dest)

        shutil.move(tmpdir, dest)

    def _ls_files(self, path, recursive=False):

        path = self._adapt_remote_path(path)
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, path)
            if recursive or '/' not in relative_path:
                files.append(relative_path)
        return files

    def _download_file(self, source, dest):
        """
        dest is a directory if ending with '/' or '.', otherwise it's a file
        """
        source = self._adapt_remote_path(source)

        if dest.endswith('/'):
            dest = os.path.join(dest, os.path.basename(source))

        logger.info(f'Downloading {source} to {dest}')
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        bc = self.client.get_blob_client(blob=source)
        with open(dest, 'wb') as file:
            data = bc.download_blob()
            file.write(data.readall())

    def _upload_file(self, source, dest, overwrite=False):
        """
        dest is a directory if ending with '/' or '.', otherwise it's a file
        """
        dest = self._adapt_remote_path(dest)
        if source.endswith('/'):
            source = os.path.join(source, os.path.basename(dest))

        bc = self.client.get_blob_client(blob=dest)
        with open(source, 'rb') as data:
            bc.upload_blob(data, overwrite=overwrite)

    def get_df(self, remote_path):
        """
        """
        remote_path = self._adapt_remote_path(remote_path)
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.download(remote_path, tmp_dir)
            logger.info('Reading dataframe from tmpdir')
            df = get_df_from_parquet_dir(os.path.join(tmp_dir, os.path.basename(remote_path)))
        return df

    def save_json(self, filepath, data):

        bc = self.client.get_blob_client(blob=self._adapt_remote_path(filepath))
        bc.upload_blob(json.dumps(data), overwrite=True)

    def load_json(self, filepath):

        bc = self.client.get_blob_client(blob=self._adapt_remote_path(filepath))
        return json.loads(bc.download_blob().readall().decode())
