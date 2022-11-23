# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import pandas as pd
import luigi

from luisy.testing import (
    luisyTestCase,
    get_all_dependencies,
)
import luisy


@luisy.raw
@luisy.csv_output(sep=';')
class RawTask(luisy.ExternalTask):
    a = luigi.IntParameter(default=2)

    def get_file_name(self):
        return f"some_export_{self.a}"


@luisy.interim
@luisy.requires(RawTask)
class InterimTask(luisy.Task):
    a = luigi.IntParameter(default=2)

    def run(self):
        df = self.input().read()
        df['C'] = (df*self.a).sum(axis=1)

        self.write(df)


@luisy.final
@luisy.requires(InterimTask)
class FinalTask(luisy.Task):

    def run(self):
        df = self.input().read()
        df = df.transpose()
        self.write(df)


@luisy.requires(RawTask)
class DictTask(luisy.Task):
    def run(self):
        dct = self.input().read()
        dct['there'][0] = 10


class TestTesting(luisyTestCase):

    def test_run(self):
        existing_outputs = [
            (RawTask(a=1), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]}))
        ]

        df = self.run_pipeline(FinalTask(a=1), existing_outputs=existing_outputs)
        pd.testing.assert_frame_equal(
            df,
            pd.DataFrame(
                data={0: [1, 3, 4], 1: [2, 4, 6]},
                index=['A', 'B', 'C']
            )
        )

    def test_with_given_interim_given(self):
        df_test = pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})
        existing_outputs = [
            (InterimTask(a=1), df_test)
        ]
        df = self.run_pipeline(FinalTask(a=1), existing_outputs=existing_outputs)
        pd.testing.assert_frame_equal(
            df,
            df_test.transpose()
        )

    def test_execution_summary_for_failed_task(self):
        existing_outputs = [
            (InterimTask(a=1), {'key': 'value'})
        ]
        summary = self.get_execution_summary(FinalTask(a=1), existing_outputs=existing_outputs)
        self.assertEqual(
            summary['failed'],
            {FinalTask(a=1)}
        )

    def test_run_for_failed_task(self):

        existing_outputs = [
            (InterimTask(a=1), {'key': 'value'})
        ]

        with self.assertRaises(AssertionError):
            self.run_pipeline(FinalTask(a=1), existing_outputs=existing_outputs)

    def test_run_with_missing_externals(self):
        with self.assertRaises(AssertionError):
            self.run_pipeline(
                task=FinalTask(a=1),
                existing_outputs=[
                    (RawTask(a=2), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})),
                    (RawTask(a=3), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]}))

                ]
            )

    def test_assert_success_for_correct_runs(self):
        self.assertSuccess(
            task=FinalTask(a=1),
            existing_outputs=[
                (RawTask(a=1), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]}))
            ]
        )

    def test_assert_fail_for_incorrect_runs(self):
        self.assertFail(
            task=FinalTask(a=1),
            existing_outputs=[
                (RawTask(a=2), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})),
            ]
        )

    def test_assert_missing_for_incorrect_runs(self):
        self.assertMissing(
            task=FinalTask(a=1),
            existing_outputs=[
                (RawTask(a=2), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})),
            ]
        )

    def test_get_execution_summary(self):
        summary = self.get_execution_summary(
            task=FinalTask(a=1),
            existing_outputs=[
                (RawTask(a=2), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})),
                (RawTask(a=3), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]}))

            ]
        )
        self.assertEqual(
            summary['upstream_missing_dependency'],
            {FinalTask(a=1), InterimTask(a=1)}
        )

    def test_get_all_dependencies(self):
        tasks = get_all_dependencies(FinalTask(a=1))
        self.assertIsInstance(tasks, set)
        self.assertEqual(
            tasks,
            {RawTask(a=1), InterimTask(a=1), FinalTask(a=1)}
        )

    def test_mocking_does_not_change_mutable_objects(self):
        existing_outputs = [
            (RawTask(a=2), pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]}))
        ]
        output = self.run_pipeline(
            task=FinalTask(a=2),
            return_store=True,
            existing_outputs=existing_outputs
        )
        pd.testing.assert_frame_equal(
            left=output[RawTask(a=2)],
            right=pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})
        )
        pd.testing.assert_frame_equal(
            left=output[FinalTask(a=2)],
            right=pd.DataFrame(data={'A': [1, 2], 'B': [3, 4], 'C': [8, 12]}).T
        )

        existing_outputs = [(RawTask(a=2), {'there': [50]})]
        output = self.run_pipeline(
            task=DictTask(),
            return_store=True,
            existing_outputs=existing_outputs
        )
        self.assertEquals(output[RawTask(a=2)]['there'][0], 50)
