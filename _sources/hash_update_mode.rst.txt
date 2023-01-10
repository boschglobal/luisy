Updating Hashes
===============

Usually :py:mod:`luisy` checks the local hashes on your system and compares them with the hashes
that are computed in the current luisy run. All tasks, whose hash changed, will be cleaned from
your system / the cloud and then re-executed on the current code base. Unfortunately refactorings
and spelling errors, which do not change the output of a task, also lead to a different hash, so
that the task has to be executed again. In these cases, sometimes the user just wants to
overwrite the old hash with the current one without starting the pipeline all over again.

luisy handles this problem by introducing a **Hash Update Mode**. Running this mode, will not
execute luigi at all. It will only scan the hashes, saved on your system, and compare them  to
the hashes of your current codebase. If luisy detects changes, it will ask you to overwrite the
hashes without executing the pipeline.

.. note::
    If there are new tasks inside your current pipeline, the hash update mode won't add these to
    your `.luisy.hash` file

To run luisy in **Hash Update Mode** use the normal command to execute your pipeline and attack
the flag `--update-hash-mode`:

.. code-block:: bash

     luisy \
         --module [project_name].[module] MyTask 
         (--working_dir=/path/to/my/data/dir) \
         --update-hash-mode

You can also combine the Hash Update Mode with the upload functionality of luisy, which also
updates all the hashes in the cloud, if desired. Just add :code:`--upload` to your call.
