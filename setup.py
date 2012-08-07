#!/usr/bin/env python
# vim: set fileencoding=utf-8
from distutils.core import setup

import sys

#import os
#
#def is_package(path):
#    return (
#        os.path.isdir(path) and
#        os.path.isfile(os.path.join(path, '__init__.py'))
#        )
#
#def find_packages(path, base="" ):
#    """ Find all packages in path """
#    packages = {}
#    for item in os.listdir(path):
#        dir = os.path.join(path, item)
#        if is_package( dir ):
#            if base:
#                module_name = "%(base)s.%(item)s" % vars()
#            else:
#                module_name = item
#            packages[module_name] = dir
#            packages.update(find_packages(dir, module_name))
#    return packages
#
#packages = find_packages(".")

setup(
    name        = "tophat",
    version     = "0.1",
    description = "An interconnection framework",
    author      = "Jordan Augé, Marc-Olivier Buob",
    url         = "http://www.top-hat.info",
    license     = "GPLv3",
    platforms   = "Linux",
    packages = [
        "tophat",
        "tophat.auth",
        "tophat.conf",
        "tophat.conf.logconfig",
        "tophat.core",
        "tophat.models",
        "tophat.gateways",
        "tophat.gateways.sfa",
        "tophat.gateways.sfa.rspecs",
        "tophat.router",
        "tophat.util" ],
    package_dir = {"": "src"},
)
