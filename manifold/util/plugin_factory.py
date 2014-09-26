#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# PluginFactory is a metaclass used including Gateway
# to implicitely registers python module according to
# a key name without having to explicitly import them.
#
# Example: Manifold Gateway uses this mechanism:
#
# 1) In manifold/gateways/__init__.py: we define the
# "__plugin__name__attribute__" used to register gateways.
#
# 2) In each manifold/gateways/*/__init__.py file: we
# define a "__gateway_name__" class attribute to register
# the Gateway.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import pkgutil, traceback
from manifold.util.log import Log

PROPERTY_NAME = '__plugin_factory_registry__'
METAPROPERTY  = '__plugin__name__attribute__'

class PluginFactory(type):
    def __init__(cls, class_name, parents, attrs):
        """
        Args:
            cls: The class type we're registering.
            class_name: A String containing the class_name.
            parents: The parent class types of 'cls'.
            attrs: The attribute (members) of 'cls'.
        """
        type.__init__(cls, class_name, parents, attrs)

        # Be sure the property storing the mapping exists
        try:
            registry = getattr(cls, PROPERTY_NAME)
        except AttributeError:
            setattr(cls, PROPERTY_NAME, {})
            registry = getattr(cls, PROPERTY_NAME)

        if not METAPROPERTY in attrs:
            # We are not in the base class: register plugin
            plugin_name_attribute = cls.get_plugin_name_attribute()
            plugin_names = attrs.get(plugin_name_attribute, [])

            if not isinstance(plugin_names, list):
                plugin_names = [plugin_names]
                
            for plugin_name in plugin_names:
                if plugin_name in registry.keys():
                    raise KeyError("Colliding plugin_name (%s) for class %s" % (plugin_name, cls))
                registry[plugin_name] = cls #class_name

        else:
            # We are in the base class
            cls.plugin_name_attribute = attrs[METAPROPERTY]

            # Adding a class method get to retrieve plugins by name
            def get(self, name):
                """
                Retrieve a registered Gateway according to a given name.
                Args:
                    name: The name of the Gateway (gateway_type in the Storage).
                Returns:
                    The corresponding Gateway.
                """
                try:
                    return registry[name]
                except KeyError:
                    import traceback
                    traceback.print_exc()
                    Log.error("Cannot find %s in {%s}" % (name, ', '.join(registry.keys())))

            setattr(cls, 'get', classmethod(get))
            setattr(cls, 'list', classmethod(lambda self: registry))
            setattr(cls, 'get_plugin_name_attribute', classmethod(lambda self: self.plugin_name_attribute))
        
        #return super(PluginFactory, cls).__new__(cls, class_name, parents, attrs)

    @staticmethod
    def register(package):
        """
        Register a module in Manifold.
        Args:
            package: a module instance.
        """
        prefix = package.__name__ + "."
        # Explored modules are automatically imported by walk_modules + it allows to explore
        # recursively manifold/gateways/
        # http://docs.python.org/2/library/pkgutil.html
        for importer, modname, ispkg in pkgutil.walk_packages(package.__path__, prefix, onerror = None):
            try:
                module = __import__(modname, fromlist = "dummy")
            except Exception, e:
                Log.warning("Cannot load %s : %s: %s" % (modname, e, traceback.format_exc()))
