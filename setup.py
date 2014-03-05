#!/usr/bin/env python
# -*- coding:utf-8 */

import os
from glob       import glob
from setuptools import find_packages, setup
from platform   import dist

import manifold

distribution, _, _ = dist()

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
# The following makes fail: "python setup.py bdist_rpm"
# The MANIFEST file is generated dynamicaly by the Makefile which is wrong, we should only write a MANIFEST.in file
# and build rpm packages using "python setup.py bdist_rpm"
#long_description = open(os.path.join(ROOT_PATH, 'README.rst')).read()
long_description = ""

etc_sysconfig = (
    "/etc/sysconfig/manifold-xmlrpc",
#    ["manifold/etc/etc_sysconfig_xmlrpc"] if distribution == "fedora" else []
)

etc_initd = (
    "/etc/init.d/manifold-xmlrpc",
#    ["manifold/etc/etc_init.d_xmlrpc-%s" % distribution]
)

name             = "manifold"
install_requires = ["cfgparse", "networkx"] if distribution == "Fedora" else ["cfgparse"]
version          = '.'.join(["%s" % x for x in manifold.__version__])
provides         = ["%(name)s (%(version)s)" % locals()]

# http://devblog.4teamwork.ch/blog/2013/06/20/setuptools-console-scripts-entrypoints-and-buildout/
entry_points = """
[console_scripts]
manifold-shell             = manifold.bin.shell:main
manifold-xmlrpc            = manifold.bin.xmlrpc:main
manifold-sfa-delegate      = manifold.bin.delegate:main
manifold-init-db           = manifold.bin.initdb:main
manifold-add-user          = manifold.bin.adduser:main
manifold-add-account       = manifold.bin.addaccount:main
manifold-add-platform      = manifold.bin.addplatform:main
manifold-disable-platform  = manifold.bin.disableplatform:main
manifold-enable-platform   = manifold.bin.enableplatform:main
manifold-upload-credential = manifold.bin.uploadcredential:main
"""

setup(
    name             = name,
    version          = version,
    description      = "Manifold Interconnection Framework",
    long_description = long_description,
    author           = "Jordan Augé, Marc-Olivier Buob",
    url              = "http://www.top-hat.info",
    provides         = provides,
    namespace_packages = ["manifold"],
    #setup_requires  = ['nose>=0.11',],
    #test_requires   = ['unittest2>=0.5.1',
    #    'mockito==0.5.1',
    #    'python-faker==0.2.3', # jordan: faker
    #    'factory-boy==1.0.0',],
    install_requires = install_requires,
    license          = "GPLv3",
    packages         = find_packages(),
    data_files       = [
        # Do not forget to reference those files in MANIFEST.in
        # Do not use symlinks with setuptool (those files will be ignored)
        # Do not use hardlinks with git (not supported)
        ('/usr/share/manifold/metadata/', glob('metadata/*.h')),
#        etc_initd,
#        etc_sysconfig,
    ],
    scripts = [
        'scripts/manifold-reset-db.sh',
        'scripts/manifold-populate-db.sh'
    ] #OBSOLETE| + glob(("clientbin/*"),
    ,
#    entry_points = {
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
#    entry_points = entry_points,
)
