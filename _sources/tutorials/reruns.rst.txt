Rerun changed tasks
-------------------

Consider the following pipeline, which we assume lives in the package
:code:`example_project`:

.. code-block:: python

   import luisy

   @luisy.raw
   @luisy.csv_output()
   class RawFileA(luisy.ExternalTask):
       def get_file_name(self):
           return 'file_a'


   @luisy.raw
   @luisy.csv_output()
   class RawFileB(luisy.ExternalTask):

       def get_file_name(self):
           return 'file_b'


   @luisy.interim
   @luisy.requires(RawFileA)
   class InterimFile(luisy.Task):

       def run(self):
           # some processings


   @luisy.final
   @luisy.requires(InterimFile, RawFileB)
   class FinalFile(luisy.Task):

       def run(self):
           # some processings



Assume the working-dir looks like this

* :code:`/projects/example_project/raw/RawFileA.csv`
* :code:`/projects/example_project/raw/RawFileB.csv`


and we invoke

.. code-block:: bash

   luisy --module example_project FinalFile

and afterwards, the following files exist:

* :code:`/projects/example_project/raw/RawFileA.csv`
* :code:`/projects/example_project/raw/RawFileB.csv`
* :code:`/projects/example_project/interim/InterimFile.csv`
* :code:`/projects/example_project/final/FinalFile.csv`
* :code:`/projects/example_project/.luisy.hash`

Now, if the code of of :code:`InterimFile` changes and the user runs

.. code-block:: bash

   luisy --module example_project FinalFile

again, the tasks :code:`InterimFile` and :code:`FinalFile` are both
executed again.


.. note::

   To just check which tasks would be executed, we can execute

   .. code-block:: bash

      luisy --module example_project FinalFile --dry-run

