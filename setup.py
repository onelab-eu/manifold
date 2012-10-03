#!/usr/bin/env python
# -*- coding:utf-8 */

import os
import tophat
from glob import glob
from setuptools import find_packages, setup

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
long_description = open(os.path.join(ROOT_PATH, 'README.rst')).read()

setup(
    name        = "tophat",
    version     = tophat.__version__,
    description = "TopHat interconnection framework",
    long_description=long_description,
    author      = "Jordan Augé, Marc-Olivier Buob",
    url         = "http://www.top-hat.info",
    #setup_requires=['nose>=0.11',],
    #test_requires=['unittest2>=0.5.1',
    #    'mockito==0.5.1',
    #    'python-faker==0.2.3', # jordan: faker
    #    'factory-boy==1.0.0',],
    install_requires=[
    #    'tornado>=1.2.1',
        'sqlalchemy>=0.7',
    #    'Elixir>=0.7.1',
    #    'restkit>=3.2.0',
    ],
    license     = "GPLv3",
    packages = find_packages(),
    data_files = [ ('/usr/share/myslice/metadata/', glob('metadata/*.xml')) ],
    entry_points={
        'console_scripts': [
            'tophat-xmlrpc = tophat.bin.xmlrpc:main',
        ],
    },

)
