# -*- coding: utf-8 -*-
from setuptools import setup

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='Post-Sale Automobile Report Redesign',
    version='0.1.0',
    description='Automobile Report using Spark',
    long_description=readme,
    author='Ben Griffith',
    author_email='bengriffith@outlook.com',
    url='https://github.com/bengriffith/post-sale-automobile-redesign',
    license=license
)

