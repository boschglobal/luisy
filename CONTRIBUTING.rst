This guide helps you to contributing code changes to luisy.

.. note::

   In case you found something that does not work as expected, please
   create an issue here on GitHub including

   * A description of the bug
   * Steps to reproduce the issue
   * The version of luisy and python that causes the problem


Add your changes
================

Once your planned update is aligned with the maintainers and a Jira
story for your change exists, you need to create a git branch through
Jira. No development should take place on the :code:`main` branch
directly! All changes have to take place on aligned feature branches
that are connected to Jira stories.

Please add code that is absolutely necessary for your changes only and
try to keep the requirements of luisy as slim as possible.

Before starting implementation, the `PEP8 recommendations
<https://www.python.org/dev/peps/pep-0008/>`_  and the `writing
guidelines <https://docs.python-guide.org/writing/style/>`_
may be a good read.

* We work in python virtual environments, a fast way of creating a
  virtual environment :code:`venv_luisy`, with the currently pinned
  dependencies is provided by :code:`make sync-venv`
* All code must be PEP8 compatible and each line of code should have
  at most :code:`100` characters.
* Bucketize your changes into modular functions where each function
  does only one thing.
* Avoid :py:func:`print` statements, use loggers of the :py:mod:`logging`
  module instead. Each luisy module should have an own logger of name
  :code:`__name__`
* Reduce inline comments to an absolute minimum. Use speaking
  variable and function names instead and document important notes in
  the doc-string of the respective method.
* Use speaking names. For instance, methods should have meaningful
  names representing their functionality like :code:`compute_score()`
  instead of simply :code:`score`. Don't fear long method names!
* In case you need utility functions, please add them to
  :py:mod:`luisy.helpers`


Coding style
------------
Good code is not code having a good wall time along, but is also is
easy to adapt, extend and maintain and most importantly, looks
beautiful!  Here are some things, that should be avoided:

Unindented parameter wrappings:

.. code-block:: python

   # Ugly,
   my_function_with_many_args(arg_a=10,
                              arg_b=12,
                              arg_c=13)

   # Beautiful, because if the length of the function name changes,
   # only one line needs to be changed
   my_function_with_many_args(
      arg_a=10,
      arg_b=12,
      arg_c=13)

Unnecessary parameters:

.. code-block:: python

   # Ugly, because readers need to memorize an additional variable
   arg_only_needed_as_argument = 1
   my_function(arg=arg_only_needed_as_argument)

   # Beautiful
   my_function(arg=1)


Unintuitive parameters:

.. code-block:: python

   # Ugly, as unintuitive
   i = 3

   # Beautiful
   number_of_elements = 3


Commit your changes
===================

Commit messages
---------------

:py:mod:`luisy` is versionized by git. Please make sure to write
clear and precise messages that explain your changes shortly.
Avoid messages like :code:`my latest edits`, :code:`fix problem`, or
:code:`code working again`. Instead, use messages like :code:`fix
float bug in csv target`, :code:`add target for .ABC files` or :code:`update
docu of decorators`.

Use imperative messages, that is, write :code:`adapt changelog to new
version` instead of :code:`changelog to new version adapted`

Please also see the official git `commit guidelines
<https://www.git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project#_commit_guidelines>`_.


Git configuration
-----------------

Please configure your git client so that your identity 

.. code-block:: bash

   git config user.name <Firstname Lastname>
   git config user.email <yourmail@yourprovider>

Test your changes
=================

The correctness of each functionality of luisy has to be assured by
unit tests, the more the better. If you add new functionality to
luisy, there need to be new tests assuring they work correctly.
In most cases, tests involving functionality of the module
:code:`luisy.MyModule.py` can be found in the testfile
:code:`tests/test_mymodule.py`. luisy uses `unittest` to write tests,
and `pytest` to run them. If you add a new module or a new
functionality within an existing module, it often make sense to set up
an own unittest class. For instance, assume that we add the following
function to :py:mod:`luisy.helpers`


.. code-block:: python

   def integer_addition(a, b):
      if type(a) != int or type(b) != int:
         raise ValueError('Both values must be integers')
      return a + b


for instance, the file
:code:`tests/test_helpers` may look like this:


.. code-block:: python

   import unittest
   import numpy as np
   from luisy.helpers import integer_addition

   class TestIntegerAddition(unittest.TestCase):

      def setUp(self):
         """
         This method is called prior to any method of this class with
         the prefix :code:`test_`. Can be used to set up objects that
         are needed in any testcase but that will be changed by the
         test.
         """
         self.large_int = 400
         self.true_float = 123.567

      def test_exception_on_floats(self):
         with self.assertRaises(ValueError):
            integer_addition(self.large_int, self.large_float)

      def test_exception_on_strings(self):
         with self.assertRaises(ValueError):
            integer_addition(self.large_int, "4000")

      def test_correct_addition(self):
         self.assertEqual(
            integer_addition(self.large_int, self.large_int),
            800
         )


To learn more on the :py:mod:`unittest` module, please see its
official `documentation <https://docs.python.org/3/library/unittest.html>`_
You may also want to have a look at :py:mod:`luisy.testing` for
utilities helping you writing tests.

.. note::

   Your changes to luisy can only be accepted if **all** unit tests of
   luisy pass, involving those checking the PEP8 conditions. This can
   checked locally running

   .. code-block:: bash

      pytest -m "not slow"


.. note::

   We recommend this `guide
   <https://docs.python-guide.org/writing/tests/>`_ on how to write tests
   in python.


Document your code
==================

It is worth noting that the documentation that explains what is going
on is as important as the software functionality added to luisy.
The documentation of luisy consists of doc-strings attached to each
class, method and function, as well as individual pieces of
documentation (like a guide on parameter tuning) which can be found
under the directory :code:`docs/`. All docstrings should follow the
`Google standard
<https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_
of python docstrings.

For instance, the documentation of a function should at least contain
descriptions on how the in- and output of the function looks like and
what the function does. Ideally, the doc-string also contains
examples how the function should be used. An optimal doc-string may
looks like this:

.. code-block:: python

   def integer_addition(a, b):
      """
      Adds two integer numbers after making sure that both are indeed
      of type integer.

      .. note::

         Some numpy integer types may cause problems

      .. todo::

         Extend to integer types of numpy

      Examples
      --------

      .. code-block:: python
         integer_addition(1, 2) # returns 3

      Args:
         a (int): An integer number
         b (int): An integer number

      Returns:
         int: The sum of both inputs

      Raises:
         ValueError: If any of the input is of non-integer type


      """
      if type(a) != int or type(b) != int:
         raise ValueError('Both values must be integers')
      return a + b

The full documentation of luisy can be build with

.. code-block:: bash

   python setup.py doc -W


The HTML-documentation is then located under
`build/sphinx/html/index.html`


.. note::

   Changes with no documentation will not be accepted. Make sure that
   your documentation builds without errors or warnings!


Bump the version
================

The luisy uses
`semantic versioning <https://semver.org/>`_ to determine how its
version should increment given a software change. Roughly speaking,
given the version number :code:`MAJOR.MINOR.PATCH`, then

* :code:`MAJOR` should be incremented if the changes make incompatible
  API changes.
* :code:`MINOR` should be incremented if new functionality is added in
  a backward compatible manner.
* :code:`PATCH` should be incremented if backwards compatible bugs are
  fixed.

The version of luisy can be adjusted by modifying the file
:code:`VERSION`. If you are unsure how your changes affect the
version number, feel free to contact one of the maintainers of luisy
to clarify this.

To document your changes on a higher level, we recommend to also write
a short description of your changes into the :code:`CHANGELOG.rst`.


Check the licensing
===================

Your contribution must be licensed under the Apache-2.0 license used
by this project. 

Copyright note
--------------

Include the following copyright notice at the head of a newly added


.. code-block:: python

   # Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
   # the repository https://github.com/boschglobal/luisy
   #
   # SPDX-License-Identifier: Apache-2.0


Sign your work
--------------

This project tracks patch provenance and licensing using the Developer
Certificate of Origin 1.1 (DCO) from 
`developercertificate.org <https://developercertificate.org>`_ and
Signed-off-by tags initially developed by the Linux kernel project.


   Developer Certificate of Origin
   Version 1.1
   
   Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
   
   Everyone is permitted to copy and distribute verbatim copies of this
   license document, but changing it is not allowed.
   
   
   Developer's Certificate of Origin 1.1
   
   By making a contribution to this project, I certify that:
   
   (a) The contribution was created in whole or in part by me and I
       have the right to submit it under the open source license
       indicated in the file; or
   
   (b) The contribution is based upon previous work that, to the best
       of my knowledge, is covered under an appropriate open source
       license and I have the right under that license to submit that
       work with modifications, whether created in whole or in part
       by me, under the same open source license (unless I am
       permitted to submit under a different license), as indicated
       in the file; or
   
   (c) The contribution was provided directly to me by some other
       person who certified (a), (b) or (c) and I have not modified
       it.
   
   (d) I understand and agree that this project and the contribution
       are public and that a record of the contribution (including all
       personal information I submit with it, including my sign-off) is
       maintained indefinitely and may be redistributed consistent with
       this project or the open source license(s) involved.

With the sign-off in a commit message you certify that you authored
the patch or otherwise have the right to submit it under an open
source license. The procedure is simple: To certify above Developer's
Certificate of Origin 1.1 for your contribution just append a line

.. code-block:: bash

   Signed-off-by: Random J Developer <random@developer.example.org>

to **every** ommit message using your real name or your pseudonym and
a valid email address.

.. note::

   If you have set your :code:`user.name` and :code:`user.email` in git
   configs you can automatically sign the commit by running the
   git-commit command with the :code:`-s` option. 


Individual vs. Corporate Contributors
-------------------------------------

Often employers or academic institution have ownership over code that
is written in certain circumstances, so please do due diligence to
ensure that you have the right to submit the code.

If you are a developer who is authorized to contribute to `luisy` on
behalf of your employer, then please use your corporate email address
in the Signed-off-by tag. Otherwise please use a personal email
address.

Maintain copyright holders
--------------------------

Each contributor is responsible for identifying themselves in the
`NOTICE.rst` file, the project's list of copyright holders and authors.
Please add the respective information corresponding to the
Signed-off-by tag as part of your first pull request.

If you are a developer who is authorized to contribute to pyLife on
behalf of your employer, then add your company / organization to the
list of copyright holders in the `NOTICE.rst` file. As author of a
corporate contribution you can also add your name and corporate email
address as in the Signed-off-by tag.

If your contribution is covered by this project's DCO's clause "(c)", please add the
appropriate copyright holder(s) to the `NOTICE.rst` file as part of
your contribution.


Open a pull request
===================

In case you

* added your necessary changes described in the issue or feature request
* wrote tests for your changes
* wrote documentation for your changes
* have adjusted the version of luisy
* have all unit tests pass locally

your changes are ready to be reviewed. Now, to have your changes merged as fast
as possible, please do the following:

* open a pull request from your feature branch to the
  `main` branch of luisy.
* add at least one of the maintainers as reviewer.
* add a small description to your pull request in case your changes
  differ from those in the corresponding Jira story or if you want to
  point the reviewers to certain aspects of your implementation.
* resolve any merge conflicts manually by merging the develop branch locally
  into your branch.
* make sure that the triggered GitHub actions pass and
  resolve problems if not.


Update the requirements
=======================

In case new libraries are needed
`(are they really?) <https://en.wikipedia.org/wiki/Dependency_hell>`_
we divide them into dependencies for the running system and those needed
for development.
Requirements for the development should be added to requirements_dev.in
and pinned only as much as needed. Requirements for the usage should go
to requirements.txt and pinning should be used as little as possible.
With :code:`make requirements` we create a exactly pinned
:code:`requirements_dev.txt` file which can be used for development and
docker images, e.g. for testing. With :code:`make update-requirements`
we can update the pinned `requirements_dev.txt` to the latest
dependencies. In the case of a conflict, this has to be resolved first.
Note: Only dependency conflicts are spotted via :code:`pip-compile`
(which is called by :code:`make (update-)requirements`) - you need to
test for errors resulting from upgraded dependencies yourself.
To update the default virtual environment to the currently pinned
libraries you can use again :code:`make sync-venv`.


How to cite luisy?
==================

.. code-block:: latex

   @misc{luisy,
      author = {{\em luisy} authors},
      title = {luisy, a python framework for reproducible and large
               scale data pipelines based on luigi},
      note = {Version X.Y.Z},
    }
