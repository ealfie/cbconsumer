#!/usr/bin/env python3

from setuptools import setup


def git_describe():
    from subprocess import Popen, PIPE
    import os

    if not os.path.isdir('.git'):
        raise ValueError('.git not found. Be sure to be inside the git repository.')

    try:
        p = Popen(['git', 'describe', '--tags', '--dirty', '--always'], stdout=PIPE)
    except EnvironmentError:
        print('ERROR: unable to run git. Are you sure it is installed?')
        raise

    git_describe_stdout = p.communicate()[0].decode().strip()
    return git_describe_stdout


setup(
    name='cbconsumer',
    version='0.dev0+{}'.format(git_describe().replace('-', '.')),
    description='Coinbase trades consumer with a processor/sampler/aggregator pipeline',
    long_description='',
    url='https://gitlab.com/ealfie/cbconsumer',
    author='Ezequiel Alfie',
    license="?",
    author_email='ealfie@gmail.com',
    classifiers=[
        'Environment :: Console',
        'Operating System :: Linux',
        'Programming Language :: Python',
    ],
    packages=[
        'cbconsumer',
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
)
