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

    def __init__(self, qualifiers, type, name, is_array = False, description = None):
        """
        Constructor
        Args:
            qualifiers: A list of String instances among "local" and "const".
                You may also pass an empty list. 
            type: A string describing the type of the field. It might be a
                custom type or a value stored in MetadataClass BASE_TYPES .
            name: The name of the field
            is_array: Indicates whether several instance of type 'type' are
                stored in this field.
                Example: const hops hop[]; /**< A path */
            description: The field description
        """
        self.type        = type
        self.name        = name
        self._is_array   = is_array
        self.description = description 
        self._is_local   = "local" in qualifiers
        self._is_const   = "const" in qualifiers

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The String instance (%r) corresponding to this Field 
        """
        return "<%s>" % self.get_name()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The String instance (%s) corresponding to this Field 
        """
        return "%s%s%s %s" % (
            "local " if self.is_local() else "",
            "const " if self.is_const() else "",
            self.get_type(),
            self.get_name()
        )

    @returns(bool)
    def __eq__(self, x):
        """
        Compare two Field
        Args:
            x: The Field instance compared to self
        Return:
            True iif x == y
        """
        if not isinstance(x, Field):
            raise TypeError("Invalid type: %r is of type %s" % (x, type(x)))
        return (
            self.is_local(),
            self.is_const(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ) == (
            x.is_local(),
            x.is_const(),
            x.get_type(),
            x.get_name(),
            x.is_array()
        )

    def __hash__(self):
        """
        Returns:
            The hash related to a Field (required to use
            a Field as a key in a dictionnary).
        """
        return hash((
            self.is_local(),
            self.is_const(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ))

    @returns(StringTypes)
    def get_description(self):
        """
        Returns:
            A String instance containing the comments related to this Field. 
        """
        return self.description

    @returns(StringTypes)
    def get_type(self):
        """
        Returns:
            A String instance containing the field type related to this Field.
        """
        return self.type

    @returns(StringTypes)
    def get_name(self):
        """
        Returns:
            A String instance containing the field name related to this Field.
        """
        return self.name

    @returns(bool)
    def is_array(self):
        """
        Returns:
            True iif this field corresponds to an array of elements. 
        """
        return self._is_array

    @returns(bool)
    def is_reference(self):
        return self.get_type() not in BASE_TYPES

    @returns(bool)
    def is_const(self):
        """
        Returns:
            True iif this field has the "const" qualifier.
        """
        return self._is_const

    @returns(bool)
    def is_local(self):
        """
        Returns:
            True iif this field has the "local" qualifier.
        """
        return self._is_local
