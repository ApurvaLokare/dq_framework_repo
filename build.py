from setuptools import setup, find_packages

setup(
	name='dq_framework',
	version='1.0',
	packages=find_packages(include=["common", "config_table_metadata","utilities","rules"]),
    include_package_data=True,
	install_requires=['pg8000'],
    package_data={"config_table_metadata": ["*.json"]}
)