#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class representing a field/member in a .h metadata file
#
#   class MyMetadataClass {
#        MyType my_metadata_field;
#   };
#
# This file is part of the TopHat project
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

class MetadataClass:
    BASE_TYPES = ['bool', 'int', 'unsigned', 'double', 'text', 'timestamp', 'interval', 'inet']

    def __init__(self, qualifier, class_name):
        """
        \brief Constructor
        \param qualifier A value among None and "onjoin"
        \param class_name The name of the class
        \param keys An array containing a set of key.
            A key is made of one or more field names (String).
        \param fields An array containing the set of Field instances related to this MetadataClass
        """
        self.qualifier  = qualifier
        self.class_name = class_name
        self.keys       = [] 
        self.fields     = []
        self.partitions = []

    def get_invalid_keys(self):
        """
        \return The keys that involving one or more field not present in the table
        """
        invalid_keys = []
        for key in self.keys:
            key_found = True
            for key_elt in key:
                key_elt_found = False 
                for field in self.fields:
                    if key_elt == field.get_name(): 
                        key_elt_found = True 
                        break
                if key_elt_found == False:
                    key_found = False
                    break
            if key_found == False:
                invalid_keys.append(key)
                break
        return invalid_keys

    def get_field_names(self):
        """
        \return The list of the fields in the MetadataClass
        """
        return [field.get_name() for field in self.fields]

    def get_invalid_types(self, valid_types):
        """
        \return Types not present in the table
        """
        invalid_types = []
        for field in self.fields:
            cur_type = field.type
            if cur_type not in valid_types and cur_type not in MetadataClass.BASE_TYPES: 
                print ">> %r: adding invalid type %r (valid_types = %r)" % (self.class_name, cur_type, valid_types)
                invalid_types.append(cur_type)
        return invalid_types

    def __repr__(self):
        """
        \return the string (%r) corresponding to this MetadataClass
        """
        return "Class(q = %r, n = %r, k = %r)\n" % (self.qualifier, self.class_name, self.keys)


