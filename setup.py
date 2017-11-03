""" Handles set up of Import.io Retry Python Package """
from setuptools import setup

setup(
    name="Import.io Retry",
    version="0.3",
    author="Ryan McNally",
    author_email="ryan.mcnally@import.io",
    description=("A package for managing failed URLs retries on the Import.io platform."),
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
