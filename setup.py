#!/usr/bin/env python3

from setuptools import setup

import versioneer


setup(
    name='cbconsumer',
    version=versioneer.get_version(),
    description='Coinbase trades consumer with a processor/sampler/aggregator pipeline',
    long_description='',
    packages=['cbconsumer'],
    url='https://github.com/ealfie/cbconsumer',
    author='Ezequiel Alfie',
    license="AGPLv3",
    author_email='ealfie@gmail.com',
    classifiers=[
        'Environment :: Console',
        'Operating System :: Linux',
        'Programming Language :: Python',
    ],
    install_requires=[
        'uvloop',
        'aiohttp',
        'aionursery',
        'websockets',
        'copra',
        'aioinflux',
        'aioitertools',
        'iso8601',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
    ],
    setup_requires=[
        'pytest-runner',
    ],
    entry_points={
        'console_scripts': ['cbconsumer=cbconsumer.app:main'],
    },
    include_package_data=True,
    cmdclass=versioneer.get_cmdclass(),
)
