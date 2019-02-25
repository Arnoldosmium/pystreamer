from setuptools import setup, find_packages
import os
import re
from os import path
from io import open

VERSION = os.environ.get("PACKAGE_VERSION", None)
try:
    assert re.match("^([0-9]+\.)+[0-9]+$", VERSION)
except Exception as e:
    print('Invalid version string: "{}"'.format(VERSION))
    raise e

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pyStreamer',

    version=VERSION,

    description='A chainable stream solution',

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

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='stream generator chain',  # Optional

    packages=find_packages(exclude=['contrib', 'docs', 'test']),

    install_requires=[],

    extras_require={
        'test': ['pytest'],
    },
)
