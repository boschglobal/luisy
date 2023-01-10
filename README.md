# luisy


[![Test
Package](https://github.com/boschglobal/luisy/actions/workflows/test_package.yml/badge.svg)](https://github.com/boschglobal/luisy/actions/workflows/test_package.yml)
[![Test
docs](https://github.com/boschglobal/luisy/actions/workflows/test_docs.yml/badge.svg)](https://github.com/boschglobal/luisy/actions/workflows/test_docs.yml)
[![PyPI](https://img.shields.io/pypi/v/luisy)](https://pypi.org/project/luisy/)

This tool is an extension for the Python Framework
[luigi](https://luigi.readthedocs.io/en/stable/) which helps to build
reproducable and complex data pipelines for batch jobs. Visit our
[docs](https://boschglobal.github.io/luisy/) to learn more!


* [How to install?](#installing)
* [How to test?](#testing)
* [How to contribute?](#contributing)
* [Third-Party licences](#3rd-party-licenses)

---


## <a name="usage">How to use?</a>

This is how an end-to-end `luisy` pipeline may look like:

```python
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
```

## <a name="installing">How to install?</a>

*Stable Branch*: `main`

Minimum python version: 3.8

Install luisy with

```bash
pip install luisy
```

## <a name="testing">How to test?</a>

To run all unittests that are inside the tests directory use the following command:

```bash
pytest
```

## <a name="contributing">How to contribute?</a>

Please have a look at our [contribution guide](https://boschglobal.github.io/luisy/contributions.html).

# <a name="3rd-party-licenses">Third-Party Licenses</a>


## Runtime dependencies 

| Name | License | Type |
|------|---------|------|
| [numpy](https://numpy.org/) | [BSD-3-Clause License](https://github.com/numpy/numpy/blob/master/LICENSE.txt) | Dependency |
| [pandas](https://pandas.pydata.org/)|[BSD 3-Clause License](https://github.com/pandas-dev/pandas/blob/master/LICENSE)| Dependency |
| [networkx](https://pypi.org/project/networkx/)| [BSD-3-Clause License](https://github.com/networkx/networkx/blob/main/LICENSE.txt) | Dependency |
| [luigi](https://pypi.org/project/luigi/)| [Apache License 2.0](https://github.com/spotify/luigi/blob/master/LICENSE) | Dependency |
| [distlib](https://pypi.org/project/distlib/)| [Python license](https://github.com/vsajip/distlib/blob/master/LICENSE.txt) | Dependency |
| [matplotlib](https://github.com/matplotlib/matplotlib)|[Other](https://github.com/matplotlib/matplotlib/tree/main/LICENSE)| Dependency |
| [azure-storage-blob](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob)|[MIT License](https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-blob/LICENSE)| Dependency |
| [tables](https://www.pytables.org/)|[BSD license](https://github.com/PyTables/PyTables/blob/master/LICENSE.txt)| Dependency |
| [pipdeptree](https://github.com/tox-dev/pipdeptree)|[MIT License](https://github.com/tox-dev/pipdeptree/blob/main/LICENSE) | Dependency |
| [requirements-parser](https://github.com/madpah/requirements-parser)|[Apache License 2.0](https://github.com/madpah/requirements-parser/blob/master/LICENSE)| Dependency |
| [pyarrow](https://github.com/apache/arrow)|[Apache License 2.0](https://github.com/apache/arrow/blob/master/LICENSE.txt)| Dependency |

## Development dependency

| Name | License | Type |
|------|---------|------|
| [sphinx](https://www.sphinx-doc.org/en/master/)|[BSD-2-Clause](https://github.com/sphinx-doc/sphinx/blob/5.x/LICENSE)| Dependency |
| [sphinx_rtd_theme](https://github.com/readthedocs/sphinx_rtd_theme)|[MIT License](https://github.com/readthedocs/sphinx_rtd_theme/blob/master/LICENSE)| Dependency |
| [flake8](https://github.com/pycqa/flake8)|[MIT License](https://github.com/PyCQA/flake8/blob/main/LICENSE)| Dependency |
| [pytest](https://docs.pytest.org)| [MIT License](https://docs.pytest.org/en/latest/license.html) | Dependency|
| [pytest-flake8](https://pypi.org/project/pytest-flake8/)| [BSD License](https://github.com/tholo/pytest-flake8/blob/master/LICENSE) | Dependency|
| [pytest-cov](https://pypi.org/project/pytest-cov/) | [MIT License](https://github.com/pytest-dev/pytest-cov/blob/master/LICENSE) | Dependency|
| [pip-tools](https://github.com/jazzband/pip-tools) | [BSD 3-Clause License](https://github.com/jazzband/pip-tools/blob/master/LICENSE) | Dependency |
