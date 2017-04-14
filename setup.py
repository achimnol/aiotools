#!/usr/bin/env python

from setuptools import setup

version = "0.1"
long_description = "Idiomatic asyncio utilities"

setup(
    name="aiotools",
    version=version,
    author="Joongi Kim",
    author_email="me@daybreaker.info",
    long_description=long_description,
    description="Idiomatic asyncio utilities",
    license="MIT",
    url="",
    platforms=['any'],
    packages=[
        'aiotools',
    ],
    python_requires='>=3.6',
    install_requires=[
    ],
    extras_require={
        'test': ['pytest', 'pytest-asyncio'],
    }
)
