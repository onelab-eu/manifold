#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Register every available Gateways.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import traceback, sys
from os                                 import sep, walk
from os.path                            import basename, dirname, join
from inspect                            import getmembers, isclass

from manifold.gateways.gateway          import Gateway
from manifold.util.log                  import Log
from manifold.util.type                 import accepts, returns

#-------------------------------------------------------------------------------
# List of gateways
#-------------------------------------------------------------------------------

@returns(bool)
def is_gateway(class_type):
    """
    Test whether a class_type corresponds to a Manifold Gateway or not.
    Returns:
        True iif a class type corresponds to a Manifold Gateway
    """
    try:
        ret = isclass(class_type) and class_type != Gateway #and issubclass(class_type, Gateway) and class_type != Gateway
    except Exception, e:
        # AttributeError: class Cache has no attribute '__class__', etc...
        ret = False
    return ret

@returns(list)
def get_class_names(module_name):
    """
    Returns:
        A list of Strings where each String is the class name of a Gateway.
    """
    __import__(module_name)
    return [class_name for class_name, class_type in getmembers(sys.modules[module_name], is_gateway) if class_name.endswith("Gateway")]

def register_gateways():
    """
    Import every *Gateway classes disseminated in manifold/gateways/*/__init__.py.
    """
    gateway_dir = dirname(__file__)
    start = len(gateway_dir) - len("manifold/gateways")

    # Build module names providing Gateways (['manifold.gateways.csv', ...])
    # We only inpect __init__.py files.
    module_names = list()
    for directory, _, filenames in walk(gateway_dir):
        if "__init__.py" in filenames:
            module_name = directory.replace(sep, ".")[start:]
            module_names.append(module_name)

    # Inspect each modules to find *Gateway classes which inherits
    # Gateway (excepted Gateway itself).
    for module_name in module_names:
        try:
            class_names = get_class_names(module_name) 
        except Exception, e:
            # An Exception has been raised, probably due to a syntax error
            # inside the Gateway sources
            Log.error(traceback.format_exc())

        # http://stackoverflow.com/questions/9544331/from-a-b-import-x-using-import
        for class_name in class_names:
            __import__(module_name, fromlist = [class_name])

__all__ = ['Gateway']

