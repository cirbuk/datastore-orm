#!/usr/bin/env python

from distutils.core import setup
import setuptools

setup(name='datastore-orm',
      version='0.0.6',
      description='datastore-orm wraps google-cloud-datastore in a lightweight ORM and helps you easily migrate away from NDB',
      author='Chaitanya Nettem',
      author_email='chaitanya@kubric.io',
      url='https://bitbucket.org/craftworkx/datastore-orm',
      install_requires=['google-cloud-datastore==1.6.0'],
      packages=['datastore_orm'])
