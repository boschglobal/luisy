Decorators
==========

luisy's core functionality is the use of decorators. Our idea is to reduce lots of boilerplate
code by offering the user various decorators to decorate his tasks.
Decorators are used for:

* Reading / Writing Tasks
* Filename generation
* Passing parameters between tasks
* Sorting tasks into `raw/interim/final` directory structure


Outputs
-------
Here are a list of decorators for the outfile of a task:

Generally, tasks with does decorators can persist their output with
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
  is a directory holding parquet files, like the result of an Apache
  Spark computation.
* :py:func:`~luisy.decorators.make_directory_output`: Factory to create
  your own directory output decorator. This method wants you to pass a
  function, which tells luisy how to handle the files in your directory.



Parquet dir output
~~~~~~~~~~~~~~~~~~

In many cases, a pyspark job writes a folder holding multiple
parquet-files into a Blob-storage. Using the
:py:func:`~luisy.decorators.parquetdir_output` together with the
cloud-synchronisation, those files can be automatically downloaded:


.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.parquetdir_output
   class SomePySparkResult(luisy.ExternalTask):

       partition = luigi.Parameter()

       def get_folder_name(self):
           return f"pyspark_result/Partition=self.partition"


This task points to the folder
:code:`[project_name]/raw/pyspark_result/` in the Blob storage holding
multiple parquet-files. The output can be used in a subsequent task as
follows:


.. code-block:: python

   @luisy.interim
   @luisy.requires(SomePySparkResult)
   class ProcessedResult(luisy.Task):

       partition = luigi.Parameter()

       def run(self):

           df = self.input().read()
           # do something
           self.write(df)

Invoking

.. code-block:: bash

   luisy --module [project_name].[module] ProcessedResult --partition=my_partition --download


will first download the parquet-files locally and then run the
subsequent task.


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
