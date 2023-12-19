
.. _databricks:

Interaction with Databricks
===========================

.. note::

   This is an experimental feature. Expect sharp edges and bugs.


Overview
--------

The prefered way to interact with databricks objects like a pyspark
cluster or delta tables is by using it in a databricks notebook. Its
also possible to connect from a local session using
:py:mod:`databricks_connect` (see :ref:`databricks-connect`).


Example pipeline
----------------

.. note::
   All task have to be implemented in a python
   package, only execution can be done via a databricks notebook.

This is how a cloud pipeline may looks like:

.. code-block:: python

   import luisy
   
   @luisy.deltatable_input(schema='my_schema', catalog='my_catalog', table_name='raw')
   @luisy.deltatable_output(schema='my_schema', catalog='my_catalog', table_name='interim')
   class TaskA(SparkTask):
   
       def run(self):
           df = self.input().read()
           df = df.drop('c')
           self.write(df)
   
   
   @luisy.requires(TaskA)
   @luisy.deltatable_output(schema='my_schema', catalog='my_catalog', table_name='final')
   class TaskB(SparkTask):
   
       def run(self):
           df = self.input().read()
           df = df.withColumn('f', 2*df.a)
           self.write(df)
   
   @luisy.requires(TaskB)
   @luisy.final
   @luisy.pickle_output
   class TaskC(SparkTask):
       def run(self):
           df = self.input().read()
           self.write(df)


Here, :code:`TaskA` and :code:`TaskB` read and write their data from
and to delta tables and process them with spark. :code:`TaskC`,
however, persists its output into a pickle file stored in dbfs.

Running a pipeline
------------------

First, the working dir needs to be set. Here, we can use databricks
file system (:code:`dbfs`) allowing to run the pipeline completely in
the cloud. The :py:class:`pyspark.SparkContext()` is automatically
propagated to :py:mod:`luisy` from the active session:

.. code-block:: python

   working_dir = "/dbfs/FileStore/my_working_dir"
   Config().set_param("working_dir", working_dir)


A given pipeline can be executed as follows:

.. code-block:: python

   build(SomeTask(), cloud_mode=True)

Here, all :py:class:`~luisy.tasks.base.SparkTask` objects use the
pyspark cluster of the databricks instance.

.. _databricks-connect:

Using databricks connect
------------------------

Using :py:mod:`databricks-connect`, cloud pipelines can be triggered
from python sessions outside of databricks. There, a local proxy for the remote spark
session from databricks is created in the local spark. First,
databricks connect needs to be installed.

.. code-block:: bash
   
   pip install databricks-connect

Make sure that the version of databricks-connect is compatible with
the spark version in the databricks cluster. 

To run the cloud pipelines locally, the following parameters need to
be set:

.. code-block:: python

   spark = DatabricksSession.builder.remote(
       host="https://adb-<...>.azuredatabricks.net",
       token="<your secret token>",
       cluster_id="<cluster id>,
   ).getOrCreate()

   Config().set_param('spark', spark) 

From there, everything works as in a databricks notebook.

.. note::

   The unity catalog needs to be enabled in your databricks instance.
