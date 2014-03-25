#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class representing a field
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import copy
from types              import StringTypes

from manifold.types     import type_by_name, BASE_TYPES
from manifold.util.type import returns, accepts 

class Field(object):

    def __init__(self, type, name, qualifiers = None, is_array = False, description = None):
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
        self._is_local   = bool(qualifiers and "local" in qualifiers)
        self._is_const   = bool(qualifiers and "const" in qualifiers)

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
#            self.is_local(),
#            self.is_const(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ) == (
#            x.is_local(),
#            x.is_const(),
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

    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dictionnary describing this Field for metadata.
        """
        field = self
        column = {
            "name"       : field.get_name(),
            "is_const"   : field.is_const(),
            "is_local"   : field.is_local(),
            "type"       : field.type,
            "is_array"   : field.is_array(),
            "description": field.get_description(),
            "default"    : "",
            #"column"         : field.get_name(),        # field(_name)
            #"description"    : field.get_description(), # description
            #"header"         : field,
            #"title"          : field,
            #"unit"           : "N/A",                   # !
            #"info_type"      : "N/A",
            #"resource_type"  : "N/A",
            #"value_type"     : "N/A",
            #"allowed_values" : "N/A",
            # ? category == dimension
        }
        return column

    #@returns(Field)
    def __or__(self, field2):
        """
        Merge two fields to produce the corresponding less
        restrictive Field.
        Args:
            field2: The Field that we try to merge with self.
        Raises:
            ValueError: if self and field2 cannot be merged.
        Returns:
            The corresponding merged Field
        """
        if   self.is_array() != field2.is_array() \
          or self.get_type() != field2.get_type() \
          or self.get_name() != field2.get_name():
            raise ValueError("Cannot %s | %s" % (self, field2))

        ret = copy.copy(self)
        if not ret.get_description():
            ret.description = field2.get_description()
        ret._is_const  &= field2.is_const()

        return ret
        
@returns(set)
def merge_fields(fields1, fields2):
    """
    Compute the intersection of two sets of Field instances.
    Args:
        fields1: A set of Field instances.
        fields2: A set of Field instances.
    Returns:
        The corresponding intersection (set of Fields)
    """

    @returns(tuple)
    def make_key(field):
        assert isinstance(field, Field)
        return (field.get_type(), field.get_name(), field.is_array())

    @returns(dict)
    def make_dict(fields):
        d = dict()
        for field in fields:
            if field.is_local(): pass
            d[make_key(field)] = field
        return d

    d1 = make_dict(fields1)
    d2 = make_dict(fields2)
    ret = set()
    for key, field1 in d1.items():
        try:
            field2 = d2[key]
            ret.add(field1 | field2)
        except KeyError, e:
            # This Field is in fields1, but not in fields2
            pass
    return ret 
