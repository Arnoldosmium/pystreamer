from setuptools import setup, find_packages
from os import path
from io import open

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pydragon',

    version='0.0.1',

    description='A chainable stream solution',

    long_description=long_description,

    long_description_content_type='text/markdown',

    url='https://github.com/Arnoldosmium/pydragon',

    author='Arnold Lin',

    # author_email=None,  # TODO: put the right email

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
