Helpers
=======

Find executed Tasks
-------------------

.. note::

   This is an experimental feature. Expect sharp edges and bugs.

Let's assume you just executed your pipeline with some values, which were not the default
values of your task. In this case, loading these tasks in a Notebook would look like this:

.. code-block:: python

    from my_project.tasks import MyTask

    MyTask(few='random', parameters='!').read()

In many cases you don't know the exact parameters that you used and luisy tells you "Task needs
to be executed first, try :py:meth:`luisy.base.Task.get_related_instances` to find your executed
instances of this task".

In this case, you can use :py:meth:`luisy.base.Task.get_related_instances` to find all executed
instances of this task on your disk (Note: Task is ideally decorated with
:py:func:`luisy.decorators.auto_filename`, or has a proper filename. else, this functionality may
not work properly.)

.. code-block:: python

    from my_project.tasks import MyTask

    MyTask.get_related_instances()
    #> [MyTask(few='random', parameters='!'), MyTask(few='default', parameters='default'), ...]
