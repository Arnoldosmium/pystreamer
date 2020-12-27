from setuptools import setup, find_packages
import os
import re
from os import path
from io import open

VERSION = "@PACKAGE_VERSION@"
if not (re.match("^([0-9]+\.)+[0-9]+(\+[0-9]+\.[0-9a-h]+)?$", VERSION) or os.environ.get("BUILD_SNAPSHOT", None)):
    raise Exception('Invalid version string: "{}"'.format(VERSION))

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pyStreamer',

    version=VERSION,

    description='A lazy evaluating, memory friendly, chainable stream solution',

    long_description=long_description,

    long_description_content_type='text/markdown',

    url='https://github.com/Arnoldosmium/pystreamer',

    download_url='https://pypi.org/project/pyStreamer/',

    author='Arnold Lin',

    author_email="contact@arnoldlin.tech",

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],

    keywords='stream generator chain',  # Optional

    packages=find_packages(exclude=['contrib', 'docs', 'test']),

    install_requires=[],

    extras_require={
        'test': ['pytest'],
    },
)
