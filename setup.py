"""
Setup file
The code is licensed under the MIT license.
"""

from os import path
from setuptools import setup, find_packages

# Content of the README file
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()

# Setup
setup(
    name='routines',
    version='0.0.1',
    author='Meteostat',
    author_email='info@meteostat.net',
    description='Import and export meteorological data using Python.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/meteostat/routines',
    keywords=['weather', 'climate', 'data', 'timeseries', 'meteorology'],
    python_requires='>=3.6.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas>=1.1',
        'pytz',
        'numpy',
        'configparser',
        'sqlalchemy',
        'mysql-connector-python',
        'lxml',
        'metar>=1.8',
        'cython',
        'pyproj',
        'netCDF4==1.5.6',
        'xarray',
        'verde==1.6.1'],
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
)
