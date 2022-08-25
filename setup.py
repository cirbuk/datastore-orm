#!/usr/bin/env python

from distutils.core import setup
import setuptools

setup(name='datastore-orm',
      version='2.0.2',
      description='datastore-orm wraps google-cloud-datastore in a lightweight ORM and helps you easily migrate away from NDB',
      author='Chaitanya Nettem',
      author_email='chaitanya@kubric.io',
      url='https://bitbucket.org/craftworkx/datastore-orm',
      install_requires=['google-cloud-datastore>=2.8.1'],
      packages=['datastore_orm'])
