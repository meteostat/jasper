from distutils.core import setup

setup(
	name='Routines',
	version='0.1',
	description='Automated routines for importing and managing weather and climate data with Python.',
	license="MIT",
	author='Meteostat',
	author_email='info@meteostat.net',
	url='https://github.com/meteostat/routines',
	packages=[],
	python_requires='>=3.3',
    install_requires=[
        "mysql-connector-python>=8.0",
    ],
)