Why luisy?
==========

- Decrease the need for boilerplate code by passing parameters
    - luisy has a smart way of passing parameters between tasks

- No more waiting for long pipelines to run
    - luisy can just download results of pipelines that were already executed by others

- No more manual implementation of write/read in tasks
    - Decorating your tasks is enough to define your read/write of your tasks

- No more manual deletion of deprecated tasks (imagine it like a git for data)
    - Hash functionality automatically detects changes in your code and deletes files that are
      created by deprecated project versions

- No more manual mocking of input/output when testing your tasks
    - Testing module allows easy and quick testing of your implemented pipelines and tasks

