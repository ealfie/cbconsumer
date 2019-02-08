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
    author_email='ealfie@gmail.com',
    license="?",
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
        'pytest-cov',
    ],
    setup_requires=[
        'pytest-runner',
    ],
    scripts=['bin/cbconsumer'],
    include_package_data=True,
    cmdclass=versioneer.get_cmdclass(),
)
