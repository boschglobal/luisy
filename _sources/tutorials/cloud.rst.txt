Cloud usage
-----------

This section shows you how to make use of download and upload
functionality that luisy offers.  To use the luisy cloud features,
make sure to have your `LUISY_AZURE_STORAGE_KEY`,
`LUISY_AZURE_ACCOUNT_NAME`, and `LUISY_AZURE_CONTAINER_NAME` set in
the environment variables of your system. More information :doc:`Cloud
synchronisation <../cloud>`.  Consider the following small pipeline in
the module :code:`luisy_playground`

.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.csv_output()
   class FileA(luisy.ExternalTask):
       def get_file_name(self):
           return 'file_a'


   @luisy.raw
   @luisy.csv_output()
   class FileB(luisy.ExternalTask):

       def get_file_name(self):
           return 'file_b'

   @luisy.interim
   @luisy.requires(FileA)
   class MultipliedFile(luisy.Task):

       def run(self):
           # some processings


   @luisy.final
   @luisy.requires(MultipliedFile, FileB, as_dict=True)
   class FinalFile(luisy.Task):

       def run(self):
           # some processings


Assume the cloud storage holds the following files:

* `/projects/luisy_playground/raw/file_a.csv`
* `/projects/luisy_playground/raw/file_b.csv`
* `/projects/luisy_playground/interim/MultipliedFile.csv`


Assume a user has no files available locally. If the user initially runs

.. code-block:: bash

   luigi --module my_module.tasks FinalFile --download --local-scheduler


then :py:mod:`luisy` first checks whether the output of `FinalFile` is
available locally. It is then checked whether it's in the cloud. It's
not, end hence its requirements, :code:`MultipliedFile` and
:code:`FileB`, are checked. Both are also not available locally, but
as they are in the cloud, they are downloaded to his local system.
Afterwards, the task :code:`FinalFile` is executed locally. Thus,
after the pipeline ran, the local file system of the user looks like
this:

* `/projects/luisy_playground/raw/file_b.csv`
* `/projects/luisy_playground/interim/MultipliedFile.csv`
* `/projects/luisy_playground/final/FinalFile.csv`


If the user now runs

.. code-block:: bash

   luisy --module my_module.tasks FinalFile --upload


the target of :code:`FinalFile` is uploaded to the cloud.

This (and this is hot stuff), allows any other user to run:


.. code-block:: python

   from my_module.tasks import FinalFile
   import luisy

   # if env variable not set
   luisy.set_working_dir('my_working_dir')

   luisy.activate_download()
   data = FinalFile().read()
