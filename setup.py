#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-hbase',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='HBase plugin for Intake',
    url='https://github.com/ContinuumIO/intake-hbase',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_hbase'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
