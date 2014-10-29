#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# dispatcher build script.
#
# Usage:
#     cd ~/git/tophat/
#     python setup.py install
#     python setup.py bdist_rpm
#
# Authors:
#    Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#    Jordan Augé <jordan.auge@lip6.fr>
#
# Copyright UPMC Paris Universitas

import os
from glob import glob
from platform   import dist
from setuptools import find_packages, setup

from manifold   import __version__

distribution, _, _ = dist()
ROOT_PATH          = os.path.abspath(os.path.dirname(__file__))
long_description   = open(os.path.join(ROOT_PATH, "README.rst")).read()
SERVICES           = ["manifold-router", "manifold-xmlrpc", "manifold-agent"]
CRONSERVICES       = ["manifold-agent"]

#-----------------------------------------------------------------------
# Those files are not *py files and must be referenced in MANIFEST.in
# Do not use symlinks with setuptool (those files will be ignored)
# Do not use hardlinks with git (not supported)
#-----------------------------------------------------------------------

data_files = [
    ("/etc/manifold", [
        "manifold/etc/manifold/manifold.conf",
    ])
]

#-----------------------------------------------------------------------
# Some data files depends on the target distribution. 
# We alter data_files consequently.
#
# Do not forget to add tdmi/agent/etc/rpm/* files
# and tdmi/agent/etc/deb/* into MANIFEST.in.
#-----------------------------------------------------------------------

def is_rpm_target():
    """
    Test whether we're building a rpm package or whether we are
    running this script on a rpm-based distribution.
    """
    import sys
    from platform   import dist

    distribution, _, _ = dist()

    if distribution == "fedora":
        return True

    for argv in sys.argv[1:]:
        # We are running a command like:
        #   python setup.py bdist_rpm
        #   python setup.py install ... --root=.../build/bdist.linux-x86_64/rpm/BUILDROOT/...  ...
        if "rpm" in argv:
            return True

    return False

is_rpm = is_rpm_target()

# Directory depending on the target linux distribution
dedicated_dir = "manifold/etc/%s" % ("rpm" if is_rpm else "deb")
config_subdir = "sysconfig" if is_rpm else "default"

# Add default configuration files in the package
data_files.append((
    "/etc/%s" % config_subdir, # for example: /etc/default 
    [
        "%(dedicated_dir)s/%(config_subdir)s/%(service)s" % {
            "dedicated_dir" : dedicated_dir,
            "config_subdir" : config_subdir,
            "service"       : service
        } for service in SERVICES
    ]
))

# Add /etc/init.d scripts in the package
data_files.append((
    "/etc/init.d/",
    [
        "%(dedicated_dir)s/init.d/%(service)s" % {
            "dedicated_dir" : dedicated_dir,
            "service"       : service
        } for service in SERVICES
    ]
))

# Add /etc/cron.hourly scripts in the package
data_files.append((
    "/etc/cron.hourly/",
    [
        "%(dedicated_dir)s/cron.hourly/%(service)s" % {
            "dedicated_dir" : dedicated_dir,
            "service"       : service
        } for service in CRONSERVICES
    ]
))

# Add metadata 
data_files.append(
    ('/usr/share/manifold/metadata/', glob('metadata/*.h'))
)

setup(
    name             = "manifold",
    version          = ".".join(["%s" % x for x in __version__]),
    description      = "Manifold Backend",
    long_description = long_description,
    author           = "TopHat team",
    author_email     = "support@top-hat.info",
    url              = "http://git.top-hat.info/?p=manifold.git",
    license          = "GPLv3",
    zip_safe         = False,
    packages         = find_packages(),
    #namespace_packages = ["manifold"],
    data_files       = data_files,
    # Those python modules must be manually installed using easy_install
    install_requires = ["cfgparse"],
    entry_points = {
        "console_scripts": [
            'manifold-add-account       = manifold.bin.addaccount:main',
            'manifold-add-platform      = manifold.bin.addplatform:main',
            'manifold-add-user          = manifold.bin.adduser:main',
            'manifold-agent             = manifold.bin.agent:main',
            'manifold-disable-platform  = manifold.bin.disableplatform:main',
            'manifold-enable-platform   = manifold.bin.enableplatform:main',
            'manifold-init-db           = manifold.bin.initdb:main',
            'manifold-router            = manifold.bin.router:main',
            'manifold-sfa-delegate      = manifold.bin.delegate:main',
            'manifold-shell             = manifold.bin.shell:main',
            'manifold-upload-credential = manifold.bin.uploadcredential:main',
            'manifold-xmlrpc            = manifold.bin.xmlrpc:main',

        ],
    },
    options = {
        'bdist_rpm':{
            'post_install'      : 'post_install',
            'post_uninstall'    : 'post_uninstall'
        }
    },
)
