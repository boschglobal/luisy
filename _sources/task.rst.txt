Basic Task Structure
====================

This section is designed to show how tasks are generally constructed in :py:mod:`~luisy`. Other
than in :py:mod:`~luigi`, luisy tasks are mainly defined through decorators. Python decorators
are adding functionality to existing functions of a class. luisy offers a wide range of
decorators that will help you to make your task as you need it.

A basic task in luigi needs to have some decorators attached to be a working task inside a luisy pipeline:

- **Output**
    Every :py:class:`luisy.tasks.base.Task` has a corresponding output, that is located
    somewhere on your local machine. The type of your output can be defined with one of the
    output decorators that luisy offers. If no output decorator is attached to your task, the
    default (:py:func:`~luisy.decorators.pickle_output`) will be used

- **Filename**
    The output decorator only tells luisy which fileending to use and how to save
    your object. The basename of your outputfile, is usually created by decorating your task with
    :py:func:`~luisy.decorators.auto_filename`. If you choose to have a specific filename, you
    have to overwrite the method :py:meth:`~luisy.tasks.base.Task.get_file_name`.

- **requires/inherits**
    These decorators are originally taken from :py:mod:`~luigi` (See `luigi
    .util <https://luigi.readthedocs.io/en/stable/api/luigi.util.html>`_). :py:mod:`~luisy` tries to
    encourage the user to use :py:func:`~luisy.decorators.requires` and
    :py:class:`~luisy.decorators.inherits` to prevent a lot of boilerplate code in your pipelines.

- **(optional) Subdir layer**
    Best practice to manage your pipeline in raw/interim/final
    subdirectories. luisy offers decorators to define tasks in their specific locations
    (:py:func:`~luisy.decorators.raw`, :py:func:`~luisy.decorators.interim`,
    :py:func:`~luisy.decorators.final`). For more information check out :doc:`getting_started
    <tutorials/getting_started>`.

