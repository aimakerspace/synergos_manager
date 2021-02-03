#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import os
from setuptools import setup

###########
# Helpers #
###########

def read(fname):
    with open(
        os.path.join(os.path.dirname(__file__), fname), 
        encoding='utf-8'
    ) as f:
        return f.read()

setup(
    name="synergos_manager",
    version="0.2.0",
    author="AI Singapore",
    author_email='synergos-ext@aisingapore.org',
    description="Interfacing package for interacting with a Synergos network",
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Image Processing',
        'Topic :: Scientific/Engineering :: Image Recognition',
        'Topic :: Text Processing :: Linguistic',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    keywords='synergos horizontal vertical federated learning job management',
    url="https://gitlab.int.aisingapore.org/aims/federatedlearning/synergos",
    license="MIT",

    packages=["synmanager"],
    python_requires = '>=3.7',
    install_requires=[
        'pika'
    ],
    include_package_data=True,
    zip_safe=False
)