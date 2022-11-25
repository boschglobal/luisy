luisy
======

`luisy` is a Python framework that extends luigi and further
simplifies building data science pipelines by reducing development
complexity. Thus, it makes **LUI**gy more ea**SY**.


An example pipeline
-------------------

This is how an end-to-end luisy pipeline may looks like:

.. code-block:: python

   import luisy
   import pandas as pd
   
   @luisy.raw
   @luisy.csv_output(delimiter=',')
   class InputFile(luisy.ExternalTask):
   	label = luisy.Parameter()
   	def get_file_name(self): 
         return f"file_{self.label}"
   
   @luisy.interim
   @luisy.requires(InputFile)
   class ProcessedFile(luisy.Task):
   	def run(self):
   		df = self.input().read()
   		# Some more preprocessings
   		# ...
   		# Write to disk
   		self.write(df)
   
   @luisy.final
   class MergedFile(luisy.ConcatenationTask):
   	def requires(self):
   		for label in ['a', 'b', 'c', 'd']:
   			yield ProcessedFile(label=label)


Learn more about luisy in our


.. toctree::
   :maxdepth: 2
   :caption: About

   Why? <about/about>
   Authors <about/authors>
   Changelog <about/changelog>
   Contributions <contributions>


.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   Getting Started <tutorials/getting_started>
   WrapperTasks and ConcatenationTasks <tutorials/parameters>
   Directory Output <tutorials/multi_file>
   Up- and Downloading Files <tutorials/cloud>
   Trigger reruns by changing code <tutorials/reruns>


.. toctree::
   :maxdepth: 2
   :caption: Modules

   Task structure <task>
   Decorators <decorators>
   Reruns <reruns>
   Testing <testing>
   Visualize <visualize>
   Cloud sync <cloud>
   Helper funcs <helpers>
   Hash Update Mode <hash_update_mode>


.. toctree::
   :caption: API
   :maxdepth: 4

   Full reference <api/modules>

.. toctree::
   :caption: Legal
   :maxdepth: 4

   License <legal/license>
   Corporate information <legal/corporate_information>
