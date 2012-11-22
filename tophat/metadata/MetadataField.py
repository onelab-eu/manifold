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
        self.is_array    = is_array
        self.description = description 

    def __repr__(self):
        """
        \return the string (%r) corresponding to this MetadataField 
        """
        return "Field(%r, %r, %r)" % (self.qualifier, self.type, self.field_name)

    def __str__(self):
        return "\n\tField(%r, %r, %r) // %r" % (self.qualifier, self.type, self.field_name, self.description)
