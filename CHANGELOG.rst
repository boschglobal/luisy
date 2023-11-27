=========
Changelog
=========

Version 1.3.0
=============
- Added :py:class:`luisy.tasks.base.SparkTask` to run tasks on a given spark object.
- Added :py:class:`luisy.targets.DeltaTableTarget` and 
  :py:class:`luisy.targets.AzureBlobStorageTarget` to read and write data to cloud storages via 
  spark.
- Added :py:class:`luisy.targets.LuisyTarget` as an abstract interface layer to luigi

Version 1.2.1
=============
- Update contribution guide
- Docu fixes
- Update cryptography version

Version 1.2.0
=============
- Change :py:class:`luisy.testing.luisyTestCase` to :py:class:`luisy.testing.LuisyTestCase`

Version 1.1.0
=============
- Soften the dependence on how the required packages are specified by
  the user. If the requirements are not specified in the
  `requirements.txt` file, they are read off from the installed
  version using `pipdeptree`

Version 1.0.1
=============

- Smaller docu fixes

Version 1.0.0
=============
- Releasing luisy
