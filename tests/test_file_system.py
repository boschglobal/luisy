# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import patch

from luisy.file_system import AzureContainer


class Client(object):
    files = [
        'project_name/raw/a.pkl',
        'project_name/raw/b.pkl',
        'project_name/interim/c.pkl',
    ]

    def list_blobs(self, name_starts_with):

        for filepath in self.files:
            if filepath.startswith(name_starts_with):
                yield {"name": filepath}


class BlobMock(object):

    def __init__(self, *args, **kwargs):
        pass

    def get_container_client(self, _):
        return Client()


class TestAzureContainer(unittest.TestCase):

    def test_exists(self):
        with patch('luisy.file_system.BlobServiceClient', new=BlobMock):

            fs = AzureContainer(key='some_key')

            self.assertTrue(fs.exists('/project_name'))
            self.assertTrue(fs.exists('/project_name/raw'))
            self.assertTrue(fs.exists('/project_name/raw/b.pkl'))

            self.assertFalse(fs.exists('/project_nam'))
            self.assertFalse(fs.exists('/project_name/raw/b'))
