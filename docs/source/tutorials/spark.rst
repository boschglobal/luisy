
.. _pyspark:

Interaction with PySpark
========================

.. note::

   This is an experimental feature. Expect sharp edges and bugs.


Overview
--------

In this tutorial, we show how tasks can be executed on a
Spark-cluster. The key difference of a :py:class:`~luisy.tasks.base.Task` and a
:py:class:`~luisy.tasks.base.SparkTask` is that the objects handling the data are
not :py:class:`pandas.DataFrame` but :py:class:`pyspark.sql.DataFrame`
objects.

Generally, a :py:class:`~luisy.tasks.base.SparkTask` creates from the
output files of its input a :py:class:`pyspark.sql.DataFrame` and
when saving to a :py:class:`luisy.targets.CloudTarget`, the respective
spark method is used. Here, the user has to make sure that the spark
cluster has the required permissions to read and write from the
respective locations. Whenever a :py:class:`~luisy.tasks.base.SparkTask` writes or reads from a 
:py:class:`luisy.targets.LocalTarget`, a serialization into a single
:py:class:`pandas.DataFrame` takes place and the user has to make sure
that data fits into memory. 


Example pipeline
----------------

This is how a :py:mod:`spark`- pipeline may looks like:

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
however, persists its output into a pickle file, which requires
:py:mod:`luisy` to serialize all the data to a
:py:mod:`pandas.DataFrame` beforehand.

Running a pipeline
------------------

When the pipeline should be executed within an active python session,
running the pipeline can be done as follows:

.. code-block:: python

   from luisy.cli import build

   build(TaskC(), cloud_mode=True)
   
In this case, the :py:class:`pyspark.SparkContext()` is automatically
propagated to :py:mod:`luisy` from the active session. Alternatively,
if a special spark context has to be used, the spark context need to
be attached to the :py:class:`~luisy.config.Config` first as follows:

.. code-block:: python


   Config().set_param('spark', some_predefined_spark_context) 


For instance, this could be a spark instance created via spark
connect:


.. code-block:: python

   spark = SparkSession.builder.remote("sc://my-cluster:15002").getOrCreate()
   Config().set_param('spark', spark) 

Be aware that all :py:class:`~luisy.targets.LocalTarget` point to
locations on the system of the python session where :py:mod:`luisy`
runs in.

.. _databricks:

Using databricks
----------------

A convinient way to interact with pyspark clusters is by using the
databricks abstraction through a databricks notebook. Its
also possible to connect from a local session using
:py:mod:`databricks_connect` (see :ref:`databricks-connect`).

.. note::

   When using :py:mod:`luisy` in a databricks cluster, additional
   charges are generated for the user. The amount of expenses depends
   among others on the cloud provider and the cluster configuration.
   :py:mod:`luisy` has no influences on the generated costs and we
   recommend to monitor cloud costs closely.


.. note::
   The tasks itself cannot be implemented within the notebook and need
   to be implemented in a standalone python package or module.  Only
   execution can be done via a databricks notebook.


Initial configuration
~~~~~~~~~~~~~~~~~~~~~

Using :py:mod:`luisy` within a databricks cluster, the databricks file
system (:code:`dbfs`) can be used as local file system allowing to run
the pipeline completely in
the cloud, even non-:py:class:`~luisy.tasks.base.SparkTask`.

.. code-block:: python

   working_dir = "/dbfs/FileStore/my_working_dir"
   Config().set_param("working_dir", working_dir)


A given pipeline can be executed as follows:

.. code-block:: python

   build(SomeTask(), cloud_mode=True)

Here, all :py:class:`~luisy.tasks.base.SparkTask` objects use the
pyspark cluster of the databricks instance.

.. _databricks-connect:

Trigger from remote
~~~~~~~~~~~~~~~~~~~

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

.. note::

   The unity catalog needs to be enabled in your databricks instance.
