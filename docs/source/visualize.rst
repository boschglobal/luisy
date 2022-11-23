Visualize
=========

The dependencies of a :py:class:`luisy.Task` can be visualized as
follows:

.. code-block:: python

   from my_module.tasks import MyTask

   task = MyTask()
   task.visualize()

The user can hand in an own axis to embed the visualization into other
plots:

.. code-block:: python

   import matplotlib.pyplot as plt

   fig, axes = plt.subplots(figsize=(15, 5), ncols=2)
   task.visualize(ax=axes[1])



For larger graphs, we recommend to write the dependency graph into an
image

.. code-block:: python

   import matplotlib.pyplot as plt

   task.visualize()
