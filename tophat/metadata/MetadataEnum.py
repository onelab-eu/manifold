#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class representing a field/member in a .h metadata file
#
#   enum MyEnum {
#       "MyValue1",
#       "MyValue2"
#   };
#
# This file is part of the TopHat project
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

class MetadataEnum:
    def __init__(self, enum_name):
        """
        \brief Constructor
        \param enum_name The name of the enum 
        """
        self.enum_name = enum_name
        self.values = []

    def __repr__(self):
        """
        \return the string (%r) corresponding to this MetadataEnum
        """
        return "Enum(n = %r, v = %r)\n" % (self.enum_name, self.values)


