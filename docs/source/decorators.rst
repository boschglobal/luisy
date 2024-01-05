Decorators
==========

luisy's core functionality is the use of decorators. Our idea is to reduce lots of boilerplate
code by offering the user various decorators to decorate his tasks.
Decorators are used for:

* Reading / Writing Tasks
* Filename generation
* Passing parameters between tasks
* Sorting tasks into `raw/interim/final` directory structure


Local targets
-------------
Here is a list of decorators for the outfile of a task:

Generally, tasks with those decorators can persist their output with
:py:func:`self.write()` and tasks requiring these tasks can read their
output with `self.input().read()`

* :py:func:`~luisy.decorators.hdf_output`: For tasks whose output is a
  two dimensional dataframe
* :py:func:`~luisy.decorators.xlsx_output`: For tasks whose output is
  an Excel-file.
* :py:func:`~luisy.decorators.csv_output`: For tasks whose output is a
  CSV file
* :py:func:`~luisy.decorators.pickle_output`: For tasks whose output
  is a pickle. May be used to (de)serialize any python object.
* :py:func:`~luisy.decorators.parquetdir_output`: For tasks whose output
  is a directory holding parquet files, like the result of a Spark computation.
* :py:func:`~luisy.decorators.make_directory_output`: Factory to create
  your own directory output decorator. This method wants you to pass a
  function, which tells luisy how to handle the files in your directory.



Parquet dir output
~~~~~~~~~~~~~~~~~~

When persisting results of a spark computation, the prefered output
format are parquet files. This may look as follows: 

.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.requires(SomeTask)
   @luisy.parquet_output('some_dir/spark_result')
   class SomePySparkResult(luisy.SparkTask):
      
      def run(self):
         df = self.read_input()
         #  do something
         self.write(df)

If the resulting path needs to be parametrized, the method
:code:`get_folder_name` needs to be implemented:

.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.requires(SomeTask)
   @luisy.parquet_output
   class SomePySparkResult(luisy.SparkTask):
      
      partition = luigi.Parameter()

      def run(self):
         df = self.read_input()
         #  do something
         self.write(df)

      def get_folder_name(self):
         return f"some_dir/spark_result/Partition=self.partition"


In some cases, only the result of a spark pipeline triggered
externally need to be further processed. Here, these files are external inputs to
the pipeline and 
:py:func:`~luisy.decorators.parquetdir_output` together with the
cloud-synchronisation can be used download these files automatically
and process them locally:

.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.parquetdir_output('some_dir/some_pyspark_output')
   class SomePySparkResult(luisy.ExternalTask):
      pass


This task points to the folder
:code:`[project_name]/raw/some_dir/some_py/` in the Blob storage holding
multiple parquet-files. 


Cloud targets
-------------

The following targets can be used in combination with a
`luisy.task.SparkTask`:

* :py:func:`~luisy.decorators.deltatable_output`: For spark tasks whose
  output should be saved in a deltatable
* :py:func:`~luisy.decorators.azure_blob_storage_output`: For spark tasks whose
  output should be saved in a azure blob storage.

See :ref:`pyspark` for more details on how to use these targets.

Cloud inputs
------------

To ease the development of cloud pipelines using
`luisy.task.SparkTask` that should access data already persisted in
cloud storages, we provide input decorators that render the usage of
:py:class:`~luisy.tasks.base.ExternalTask` unnecessary:

* :py:func:`~luisy.decorators.deltatable_input`: For spark tasks that should acess
  data saved in a deltatable
* :py:func:`~luisy.decorators.azure_blob_storage_input`: For spark tasks whose
  output should be saved in a azure blob storage.

See :ref:`pyspark` for more details on how to implement pipelines
using these inputs.


.. note::

   Make sure that the `delta-spark` extension is installed into your
   spark cluster. See more `here <https://docs.delta.io>`_.

Directory structure
-------------------
The following decorators denote the directory in the project directory

* :py:func:`~luisy.decorators.raw`: For tasks whose output is in
  :code:`working_dir/project_name/raw`
* :py:func:`~luisy.decorators.interim`: For tasks whose output is in
  :code:`working_dir/project_name/interim`
* :py:func:`~luisy.decorators.final`: For tasks whose output is in
  :code:`working_dir/project_name/final`


The :code:`project_name` dir-layer can be modified using
:py:func:`~luisy.decorators.project_name`

.. automodule::
   luisy.decorators
   :noindex:
