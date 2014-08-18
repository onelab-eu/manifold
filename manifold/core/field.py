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
        Compare two Field instances
        Args:
            x: The Field instance compared to self
        Return:
            True iif self == x
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

@returns(Field)
def merge_field(field1, field2):
    """
    Merge two Field instances.
    Args:
        field1: A Field instance.
        field2: A Field instance.
    Raises:
        ValueError: raised if field1 and field2 share the same name
            but are contradictory.
    Returns:
        None if field1 and field2 cannot be merged (not same name)
        The resulting merged Field otherwise.
    """
    if field1 == field2:
        return field2

    if field1.get_name() != field2.get_name():
        # field1 and field2 cannot be merged
        return None

    # If we're here field1 and field2 should merge.
    # We now compute what would be the result of this merging.

    # Check consistency
    if     field1.get_type() != field2.get_type()\
        or field1.is_array() != field2.is_array()\
        or field1.is_local() != field2.is_local():
        raise ValueError("%(field1)s and %(field2)s have inconsistent types" % locals())

    # Craft qualifiers
    qualifiers = list()
    if field1.is_const() and field2.is_const():
        qualifiers.append("const")
    if field1.is_local() and field2.is_local():
        qualifiers.append("local")

    # Craft description
    desc1 = field1.get_description().strip()
    desc2 = field2.get_description().strip()
    desc  = None
    if not desc1 and desc1:
        desc = desc1
    if not desc1 and desc2:
        desc = desc2
    if desc1 and desc2 and desc1 != desc2:
        raise ValueError("%(field1)s and %(field2)s have inconsistent description (%(desc1)s) (%(desc2)s)" % locals())

    # Ok, the merge has succeed: override field1 by "field1 merge field2"
    return Field(
        qualifiers  = qualifiers,
        type        = field1.get_type(),
        name        = field1.get_name(),
        is_array    = field1.is_array(),
        description = desc 
    )

