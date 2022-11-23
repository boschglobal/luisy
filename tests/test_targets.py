# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
import os

from luisy.targets import JSONTarget


class TestTargets(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_json_output(self):
        dct = {
            'some': 'dir',
            'with': 'strings'
        }
        target = JSONTarget(path=os.path.join(self.tmpdir.name, 'test.json'))
        target.write(dct)
        loaded_dct = target.read()
        self.assertDictEqual(dct, loaded_dct)
