#!/usr/bin/env python

from setuptools import setup

version = "0.1.1"

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""

setup(
    name="aiotools",
    version=version,
    author="Joongi Kim",
    author_email="me@daybreaker.info",
    long_description=long_description,
    description="Idiomatic asyncio utilities",
    license="MIT",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Framework :: AsyncIO',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development',
    ],
    url="https://github.com/achimnol/aiotools",
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
