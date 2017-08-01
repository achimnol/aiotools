#!/usr/bin/env python

from setuptools import setup

version = "0.4.2"

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""

build_requires = [
    'pypandoc',
    'wheel',
    'twine',
]

test_requires = [
    'pytest>=3.1',
    'pytest-asyncio~=0.5.0',
    'pytest-cov',
    'codecov',
    'flake8',
]

dev_requires = [
    'pytest-sugar',
]

ci_requires = [
]

docs_requires = [
    'sphinx',
    'sphinx-autodoc-typehints',
    'guzzle-sphinx-theme',
]


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
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'ci': ci_requires,
        'docs': docs_requires,
    }
)
