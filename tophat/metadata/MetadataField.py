#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class representing a field/member in a .h metadata file
#
#   class MyMetadataClass {
#        MyType my_metadata_field;
#   };
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                         import StringTypes
from tophat.util.type              import returns, accepts 

class MetadataField:
    def __init__(self, qualifier, type, field_name, is_array = False, description = None):
        """
        \brief Constructor
        \param qualifier A value among None and "const"
        \param type A string describing the type of the field. It might be a
            custom type or a value stored in MetadataClass BASE_TYPES .
        \param field_name The name of the field
        \param is_array Indicates whether several instance of type 'type' are
            stored in this field.
            Example: const hops hop[]; /**< A path */
        \param description The field description
        """
        self.qualifier   = qualifier
        self.type        = type
        self.field_name  = field_name
        self._is_array   = is_array
        self.description = description 

    @returns(str)
    def __repr__(self):
        """
        \return the string (%r) corresponding to this MetadataField 
        """
        return "<%s>" % self.get_name()

    @returns(str)
    def __str__(self):
        """
        \return the string (%s) corresponding to this MetadataField 
        """
        return "\n\tField(%r, %r, %r) // %r" % (
            self.get_qualifier(),
            self.get_type(),
            self.get_name(),
            self.get_description()
        )

    @returns(bool)
    def __eq__(self, x):
        """
        \brief Compare two MetadataField
        \param x The MetadataField instance compared to self
        \return True iif x == y
        """
        if not isinstance(x, MetadataField):
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
        \return The hash related to a MetadataField (required to use
            a MetadataField as a key in a dictionnary)
        """
        return hash((
            self.get_qualifier(),
            self.get_type(),
            self.get_name(),
            self.is_array()
        ))

    @returns(str)
    def get_description(self):
        return self.description

    def get_qualifier(self):
        return self.qualifier

    @returns(str)
    def get_type(self):
        return self.type

    @returns(str)
    def get_name(self):
        return self.field_name

    @returns(bool)
    def is_array(self):
        return self._is_array
