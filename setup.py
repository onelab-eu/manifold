#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# dispatcher build script.
#
# Usage:
#     cd ~/git/tdmi/packages/dispatcher
#     python setup.py install 
#     python setup.py bdist_rpm
#
# Authors:
#    Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright UPMC Paris Universitas

import os
from platform   import dist
from setuptools import find_packages, setup

import manifold

distribution, _, _ = dist()                                       
ROOT_PATH          = os.path.abspath(os.path.dirname(__file__))
long_description   = open(os.path.join(ROOT_PATH, "README.rst")).read()

etc_sysconfig = (
    "/etc/sysconfig/manifold-xmlrpc",
    ["manifold/etc/etc_sysconfig_xmlrpc"] if distribution == "fedora" else []
)

etc_initd = (
    "/etc/init.d/manifold-xmlrpc",
    ["manifold/etc/etc_init.d_xmlrpc-%s" % distribution]
)

setup(
    name               = "manifold",
    version            = ".".join(["%s" % x for x in manifold.__version__]),
    description        = "Manifold Backend",
    long_description   = long_description,
    author             = "TopHat team",
    author_email       = "support@top-hat.info",
    url                = "http://git.top-hat.info/?p=manifold.git",
    license            = "GPLv3",
    zip_safe           = False,
    packages           = find_packages(),
    namespace_packages = ["manifold"],
    data_files = [
        # Do not forget to reference those files in MANIFEST.in
        # Do not use symlinks with setuptool (those files will be ignored)
        # Do not use hardlinks with git (not supported)
        ("/etc/manifold", [
            "manifold/etc/manifold_manifold.conf",
        ]),
        etc_initd,
        etc_sysconfig
    ],
    install_requires   = ["setuptools", "cfgparse", "python-daemon"],
    entry_points = {
        "console_scripts": [
            'manifold-shell = manifold.bin.shell:main',
            'manifold-xmlrpc = manifold.bin.xmlrpc:main',
            'manifold-sfa-delegate = manifold.bin.delegate:main',
            'manifold-init-db = manifold.bin.initdb:main',
            'manifold-add-user = manifold.bin.adduser:main',
            'manifold-add-account = manifold.bin.addaccount:main',
            'manifold-add-platform = manifold.bin.addplatform:main',
            'manifold-disable-platform = manifold.bin.disableplatform:main',
            'manifold-enable-platform = manifold.bin.enableplatform:main',
            'manifold-upload-credential = manifold.bin.uploadcredential:main',

        ],
    },
)
