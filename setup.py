from setuptools import (
    setup,
    find_packages,
)
from pathlib import Path


with open('requirements.txt') as f:
    requirements_install = f.read().splitlines()

with open('VERSION') as f:
    version = f.read().splitlines()[0]

setup(
    name='luisy',
    version=version,
    description='Framework to build data pipelines',
    author='Robert Bosch GmbH',
    entry_points={
        'console_scripts': [
            'luisy = luisy.cli:luisy_run',
        ]
    },
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=requirements_install,
    tests_require=[],
    license="Apache-2.0",
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type='text/markdown',
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },
    project_urls={}
)
