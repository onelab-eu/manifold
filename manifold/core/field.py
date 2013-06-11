#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class representing a field
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types              import StringTypes
from manifold.util.type import returns, accepts 
from manifold.util.log  import Log
from manifold.types     import type_by_name, BASE_TYPES


class Field(object):


    def __init__(self, qualifier, type, name, is_array = False, description = None, local=False):
        """
        \brief Constructor
        \param qualifier A value among None and "const"
        \param type A string describing the type of the field. It might be a
            custom type or a value stored in MetadataClass BASE_TYPES .
        \param name The name of the field
        \param is_array Indicates whether several instance of type 'type' are
            stored in this field.
            Example: const hops hop[]; /**< A path */
        \param description The field description
        """
        self.qualifier   = qualifier
        self.type        = type
        self.name        = name
        self._is_array   = is_array
        self.description = description 
        self.local       = False

    @returns(StringTypes)
    def __repr__(self):
        """
        \return the string (%r) corresponding to this Field 
        """
        return "<%s>" % self.get_name()

    @returns(StringTypes)
    def __str__(self):
        """
        \return the string (%s) corresponding to this Field 
        """
        return "%s%s%s %s" % (
            "local " if self.is_local() else "",
            "%s " % self.get_qualifier() if self.get_qualifier() != None else '',
            self.get_type(),
            self.get_name()
        )

    @returns(bool)
    def __eq__(self, x):
        """
        \brief Compare two Field
        \param x The Field instance compared to self
        \return True iif x == y
        """
        if not isinstance(x, Field):
            raise TypeError("Invalid type: %r is of type %s" % (x, type(x)))
        return (
            self.get_qualifier(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ) == (
            x.get_qualifier(),
            x.get_type(),
            x.get_name(),
            x.is_array()
        )

    def __hash__(self):
        """
        \return The hash related to a Field (required to use
            a Field as a key in a dictionnary)
        """
        return hash((
            self.get_qualifier(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ))

    @returns(StringTypes)
    def get_description(self):
        return self.description

    def get_qualifier(self):
        return self.qualifier

    @returns(StringTypes)
    def get_type(self):
        return self.type

    @returns(StringTypes)
    def get_name(self):
        if not self.is_reference():
            return self.name
        # If the field is a reference to another table, we need to retrieve the key of this table
        return self.name

    @returns(bool)
    def is_array(self):
        return self._is_array

    @returns(bool)
    def is_reference(self):
        return self.get_type() not in BASE_TYPES

    @returns(bool)
    def is_local(self):
        return self.local
