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
        assert isinstance(platform, StringTypes):
            raise TypeError("Invalid platform %r (type = %r)" % (platform, type(platform)))
        if not isinstance(name, StringTypes):
            raise TypeError("Invalid name %r (type = %r)" % (name, type(name))) 

    def __init__(self, platform, name):
        """
        Constructor/
        Args:
            platform: The name of the platform provinding this method
            name: The name of the method.
                This is the name of corresponding table announced by this platform.
        """
        Method.check_init(platform, name)
        self.platform = platform
        self.name = name

    @returns(StringTypes)
    def get_platform(self):
        raise RuntimeError("Method: Use get_platforms instead()")
        return self.platform

    @returns(StringTypes)
    def get_name(self):
        """
        Returns:
            A String instance containing the (Table) name of this Method instance.
        """
        return self.name

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Method instance
        """
        return repr(self)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Method instance
        """
        return "%s::%s" % (self.get_platform(), self.get_name())

    def __hash__(self):
        """
        Returns:
            The hash of this Method instance.
        """
        return hash((self.get_platform(), self.get_name()))

    @returns(bool)
    def __eq__(self, method):
        """
        Compare self and another Method instance.
        Args:
            method: A Method instance.
        Returns:
            True iif x and self are equal.
        """
        return self.get_platform() == method.get_platform() \
           and self.get_name()     == method.get_name()

