#!/usr/bin/env python
# -*- coding:utf-8 */

import os
import manifold
from glob import glob
from setuptools import find_packages, setup
import platform

distribution, _, _ = platform.dist()                                       

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))

# The following makes fail: python setup.py bdist_rpm 
#long_description = open(os.path.join(ROOT_PATH, 'README.rst')).read()
long_description = ""

setup(
    name        = "manifold",
    version     = '.'.join(["%s" % x for x in manifold.__version__]),
    description = "MANIFOLD interconnection framework",
    long_description=long_description,
    author      = "Jordan Aug<C3><A9>, Marc-Olivier Buob",
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
    #    'twisted',
    #    'BeautifulSoup',
    #    'sfa', 'sfa-common', 'sfa-plc'
    ],
    license     = "GPLv3",
    packages = find_packages(),
    data_files = [ ('/usr/share/manifold/metadata/', glob('metadata/*.h')) ] +
        ( [ ('/etc/init.d/manifold', 'manifold/bin/etc_init.d_manifold-fedora') ] if distribution == 'fedora' else [] ) +
        #( [ ('/etc/init.d/manifold', 'manifold/bin/etc_init.d_manifold-debian') ] if distribution == 'debian' else [] ) +
         [],
    scripts = glob("clientbin/*") + ['scripts/manifold-reset-db.sh', 'scripts/manifold-populate-db.sh'],
#    entry_points={
#        'console_scripts': [
#            'manifold-shell = manifold.bin.shell:main',
#            'manifold-xmlrpc = manifold.bin.xmlrpc:main',
#            'manifold-sfa-delegate = manifold.bin.delegate:main',
#            'manifold-init-db = manifold.bin.initdb:main',
#            'manifold-add-user = manifold.bin.adduser:main',
#            'manifold-add-account = manifold.bin.addaccount:main',
#            'manifold-add-platform = manifold.bin.addplatform:main',
#            'manifold-disable-platform = manifold.bin.disableplatform:main',
#            'manifold-enable-platform = manifold.bin.enableplatform:main',
#            'manifold-upload-credential = manifold.bin.uploadcredential:main',
#        ],
#    },

)
