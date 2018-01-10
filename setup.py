#! /usr/bin/env python3

from setuptools import setup
from pathlib import Path
import re


def get_src_version():
    p = (Path(__file__).parent / 'aiotools' / '__init__.py')
    src = p.read_text()
    m = re.search(r"^__version__\s*=\s*'([^']+)'", src, re.M)
    return m.group(1)


root = Path(__file__).resolve().parents[0]

build_requires = [
    'wheel',
    'twine',
]

test_requires = [
    'pytest>=3.3',
    'pytest-asyncio~=0.8.0',
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
    version=get_src_version(),
    author="Joongi Kim",
    author_email="me@daybreaker.info",
    long_description='\n\n'.join([(root / 'README.rst').read_text(),
                                  (root / 'CHANGES.rst').read_text()]),
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
