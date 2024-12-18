Cloud synchronisation
=====================

.. note::

   This is an experimental feature. Expect sharp edges and bugs.

Each :py:class:`luisy.Task` comes with the parameters :code:`upload`
and :code:`download` that allow to synchronize the target of a task
with the Azure cloud.


How the synchronisation works
-----------------------------

If a :py:mod:`luisy` pipeline is invoked the
following steps are performed subsequently:

* Starting at the root-task given by the user, all upstream tasks
  are detected and their hash is determined using the local
  code-base (see also :ref:`rerun` for more details).
* Compared with the hashes in the local filesystem, the tasks whose
  hashes have changed are detected.
* All local files whose tasks need a rerun are deleted
* Using luigi, the pipeline is executed. If :code:`--download` is
  used, :py:mod:`luisy` checks for each task whether a file with the
  correct hash resides in the cloud. If so, the file is downloaded
  instead of executing the task locally.
* After the pipeline has run through, **all** files of the pipeline whose
  hash is different from the hash in the cloud are uploaded if
  :code:`--upload` is specified.


.. note::

   Notice that :code:`--download` and :code:`--upload` are asymmetric to some
   extend: Only what is needed is downloaded but everything is
   uploaded. The reason is that we want to minimize the data on the
   clients locally but everything should be available in the cloud
   storage.

.. note::
   
   As any other synchronization-tool, up- and downloading to your
   cloud storage system creates additional costs for you at your cloud
   provider service. Dependend on your cloud service, the costs
   can depend on the file sizes as well as on the number of files.
   luisy has no control over and is not in charge for these costs.


Prerequisites
-------------

To use this service, the access token for the 
the storage has to be set:

.. code-block:: bash

   export LUISY_AZURE_STORAGE_KEY=SECRET_KEY
   export LUISY_AZURE_CONTAINER_NAME=CONTAINER_NAME
   export LUISY_AZURE_ACCOUNT_NAME=ACCOUNT_NAME


.. note::

   The account name can be read of the Azure URL:  https://[ACCOUNT_NAME].blob.core.windows.net

.. warning::
   Dont add this to your .bashrc or something similar. The secret key is sensitive and should
   never be stored unencrypted. By adding a space in front of the `export` command, you ensure
   that this command will not be saved inside your `.bash_history`.

Here, we use the container :code:`projects`. There, a folder of the
name of the project (that is, the python package where the task lives
in) has to be created.


Downloading
-----------

If :code:`--download` is added to task execution, like

.. code-block:: bash

   luisy --download --module my_module.tasks MyTask


then if the task is not yet completed (that is, if its outfile does
not exists), then it is checked whether the file exists in the cloud.
If the file exists in the cloud, then the file is downloaded and the
task is marked as complete.


Uploading
---------

If :code:`--upload` is added to task execution, like

.. code-block:: bash

   luisy --upload --module my_module.tasks MyTask

then the result of the task is uploaded to the cloud. This can also be
added to tasks that have already been executed, in this case, just the
upload is done.

Tree traverse
-------------

If a task is called with both :code:`--upload` and :code:`--download`,
like

.. code-block:: bash

   luisy --upload --download --module my_module.tasks MyTask

then uploading and downloading is done recursively through the tree
for all tasks whose completion is checked, provided all involved tasks
are :py:class:`luisy.Tasks` that correctly forward the parameters,
like with :py:class:`luisy.requires` and :py:class:`luisy.inherits`.


