Testing
=======

The module :py:mod:`luisy.testing` provides helpers to test pipelines
end-to-end for test szenarious.


Run a pipeline 
--------------

This is how a test case may look like this:

.. code-block:: python

   from luisy.testing import luisyTestCase

   from my_project.tasks import (
       MyFinalTask,
       MyRawTask,
       MyOtherRawTask,
   )


   class TestMyPipeline(luisyTestCase):

      def test_success(self):
          self.assertSuccess(
              task=MyFinalTask(a=1, b=2),
              existing_outputs=[
                  (MyRawTask(a=1), {'some_data': 1}),
                  (MyRawTask(a=2), {'some_data': 2}),
                  (MyOtherRaw(a=2), df_test),
              ]
           )

      def test_fail(self):
          self.assertFail(
              task=MyFinalTask(a=1, b=2),
              existing_outputs=[
                  (MyRawTask(a=1), {'some_data': 1}),
              ]
           )

      def test_missing(self):
          self.assertMissing(
              task=MyFinalTask(a=1, b=2),
              existing_outputs=[
                  (MyRawTask(a=1), {'some_data': 1}),
              ]
           )

      def test_output(self):
          task_output = self.run_pipeline(
              task=MyFinalTask(a=1, b=2),
              existing_outputs=[
                  (MyRawTask(a=1), {'some_data': 1}),
                  (MyRawTask(a=2), {'some_data': 2}),
                  (MyOtherRaw(a=2), df_test),
              ]
           )

           # Performe some asserts on task_output 


Using :py:func:`luisy.testing.luisyTestCase.run_pipeline`, the user
can specify a task she would like to run and can provide outputs of
some tasks that may be needed during execution. Here, the user can
provide the output objects of the tasks as python objects using
:code:`existing_outputs`, which is a list of tuples the task and output
objects tuples


Test the execution summary
---------------------------

The most prominent test examples involve incorrect runs. For those, we
would like to know which tasks fail. For this the helper
:py:func:`luisy.testing.luisyTestCase.get_execution_summary` can be
used to get the summary of the run:

.. code-block:: python

   class TestMyPipeline(luisyTestCase):

      def test_summary(self):
          summary = self.get_execution_summary(
              task=MyFinalTask(a=1, b=2),
              existing_outputs=[
                  (MyRawTask(a=1), {'some_data': 1}),
              ]
           )

        self.assertEquals(
            summary['upstream_missing_dependency'],
            {MyFinalTask(a=1, b=2)}
        )


The returned :code:`summary` is a :py:class:`dict` holding the status
of the tasks to be runned which then can be asserted by the user.

Example
-------

Consider the following pipeline

.. code-block:: python

   import pandas as pd
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



A testcase for :code:`FinalTask` which just does a transpose  may look
like this:

.. code-block:: python

   class TestFinalTask(luisyTestCase):
   
       def test_run(self):

           df_test = pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})
           existing_outputs = [
               (InterimTask(a=1), df_test)
           ]
   
           df = self.run_pipeline(FinalTask(a=1), existing_outputs=existing_outputs)
           pd.testing.assert_frame_equal(
               df,
               df_test.transpose()
           )


Testing the pipeline from :code:`RawTask` to :code:`FinalTask`, the
user only has to give a valid output for the :code:`RawTask`:


.. code-block:: python

   class TestFinalTask(luisyTestCase):

      def test_run(self):
          df_test = pd.DataFrame(data={'A': [1, 2], 'B': [3, 4]})

          df = self.run_pipeline(
              task=FinalTask(a=1), 
              existing_outputs=[
                  (RawTask(a=1), df_test)
              ]
          )
          pd.testing.assert_frame_equal(
              df,
              pd.DataFrame(
                  data={0: [1, 3, 4], 1: [2, 4, 6]},
                  index=['A', 'B', 'C']
              )
          )
