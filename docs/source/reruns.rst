Automatic rerun detection
=========================

.. _rerun:

Overview
--------

Depending on how many people are working on the same pipeline, manual
deletions can get annoying quite fast. Therefore implemented in each
:py:class:`luisy.tasks.base.Task` a feature to detect whether it need
to be rerun based on code changes.

More specific, whenever a :py:mod:`luisy` pipeline has been successfully
executed, a hash of the code of all executed tasks is computed and
stored into a :code:`.luisy.hash` file located in the project directory
and whenever a :py:mod:`luisy` pipeline is executed, the hashes of the
tasks computed at runtime are compared with the persisted hashes.
If code of a task changes then its hash changes, the task is executed
again, and its outfile is overwritten. Moreover, all the downstream
dependencies of this task are re-run as well as their input may
changes.

Long story short: **You will never have to manually delete files anymore**


Mechanics of the hash computation
---------------------------------
The first idea is to create a hash value out of the sourcecode of every task.
However it is not enough to only capture the code of the task itself, we also need to to capture:

* the sourcecode of functions, classes and constants used in the source code of the task

* versions of external libraries used

Core Algorithm
~~~~~~~~~~~~~~
Using pythons AST library, we analyse the source code inside the body of the task class.
With AST, we can get all variable names, that are used inside the class body.
We also obtain all the local variables which are assigned (stored) inside the class body.
This way we can identify variable names that are

* used inside the class body
* not defined/assigned inside the class body

These variables must be coming from outside the class body.
Now we can check which of these three cases applies:

* Import from an external library
* Import from another module of the same package as our task is in
* Variable is a function, class or constant from the same module as our task is in

For the last two cases, we can just apply the same step recursively and add the sourcecode to a list.
Whenever we hit external dependencies, we collect their version info from the :code:`requirements.txt`
and add it to a list.
When the recursive algorithm has stopped, we generate a hash of all the source code and version
info that we have collected.


Dealing with external requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
First, find all the external packages that our task uses.
If a package (package A) is not listed in `requirements.txt` directly, we look for  another
package (package B) that requires package A and is itself listed in `requirements.txt`. Then we
can include the version info of package B in the hash.

Usage
~~~~~
If we want to create a hash of our luisy task and plot the dependency graph of AST nodes, we
simply use:

.. code-block:: python

   from luisy.code_inspection import create_hash
   task = MyTaskClass()
   hash = create_hash(task, "plot_path")

where :code:`plot_path` is the path of the output jpeg plot of the dependency graph.

Guidelines when creating the code of your task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use the task hash functionality most efficiently, note the following points:

* Do not shadow variables from module scope in class- or function scope. If you shadow a module
  level variable with a class-or function-level one, changes in the former will not be detected.
* Try to import only what you need, not the whole module. Otherwise a tiny change in the imported
  module changes the hash of your task and leads to re-execution.
* Star imports are not supported. They will make the hash creation fail.
* When using :code:`eval`, the variables used inside the the expression will not be tracked.
