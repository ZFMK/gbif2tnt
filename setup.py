import os

from setuptools import setup, find_packages


requires = [
    'pymysql',
    'pyodbc',
    'pudb',
    'configparser'
    ]

setup(name='gbif2tnt',
      version='0.1',
      description='Copy data from GBIF backbone taxonomy to DiversityWorkbench TaxonNames database',
      author='Björn Quast',
      author_email='bquast@leibniz-zfmk.de',
      install_requires=requires
      )
