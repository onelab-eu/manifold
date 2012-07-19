#!/usr/bin/env python
# vim: set fileencoding=utf-8
from distutils.core import setup
import sys

setup(
        name        = "tophat",
        version     = "0.1",
        description = "An interconnection framework",
        author      = "Jordan Augé, Marc-Olivier Buob",
        url         = "http://www.top-hat.info",
        license     = "GPLv3",
        platforms   = "Linux",
        packages    = [
            "tophat",
            "tophat.auth",
            "tophat.core",
            "tophat.gateways",
            "tophat.gateways.sfa",
            "tophat.util" ],
        package_dir = {"": "src"},
    )
