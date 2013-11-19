#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# class Method 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                         import StringTypes
from manifold.util.type            import returns, accepts

class Method(object):
    @staticmethod
    #@accepts(StringTypes, StringTypes)
    def check_init(platform, name):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        if not isinstance(platform, StringTypes):
            raise TypeError("Invalid platform %r (type = %r)" % (platform, type(platform)))
        if not isinstance(name, StringTypes):
            raise TypeError("Invalid name %r (type = %r)" % (name, type(name))) 

    def __init__(self, platform, name):
        """
        \brief Constructor
        \param platform The name of the platform provinding this method
        \param name The name of the method.
            This is the name of corresponding table announced by this platform.
        """
        Method.check_init(platform, name)
        self.platform = platform
        self.name = name

    @returns(StringTypes)
    def get_platform(self):
        return self.platform

    @returns(StringTypes)
    def get_name(self):
        return self.name

    @returns(StringTypes)
    def __str__(self):
        """
        \return The (verbose) string representing a Method instance
        """
        return self.__repr__()

    @returns(StringTypes)
    def __repr__(self):
        """
        \return The (synthetic) string representing a Method instance
        """
        return "%s::%s" % (self.get_platform(), self.get_name())

    def __hash__(self):
        return hash((self.get_platform(), self.get_name()))

    @returns(bool)
    def __eq__(self, x):
        return self.get_platform() == x.get_platform() and self.get_name() == x.get_name()

