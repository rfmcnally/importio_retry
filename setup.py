""" Handles set up of Import.io Retry Python Package """
import os
from setuptools import setup

def read(fname):
    """ Function for reading files """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="Import.io Retry",
    version="0.3",
    author="Ryan McNally",
    author_email="ryan.mcnally@import.io",
    url="https://github.com/rfmcnally/importio_retry",
    description=("A package for managing failed URLs retries on the Import.io platform."),
    long_description=read('README.md'),
    license="ASL",
    platforms="any",
    packages=['importio_retry'],
    entry_points={
        'console_scripts': [
            'importio-retry=importio_retry.importio_retry:main'
        ]
    },
    install_requires=[
        'requests >= 2.18.4',
        'pandas >= 0.21.0'
    ]
)
