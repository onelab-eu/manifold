#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Key:
#   Represent a key of a Table instance
#   A key is a set of (eventually one) MetadataFields.
#   \sa tophat/core/table.py
#   \sa tophat/metadata/MetadataField.py
# Class Keys:
#   A Table instance carries a Keys instance, e.g a set of Key instances.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from tophat.util.type              import returns, accepts
from tophat.metadata.MetadataField import MetadataField
from types                         import StringTypes

class Key(frozenset):
    """
    Implements a key for a table.
    A key is a set of (eventually one) MetadataFields.
    """

    @staticmethod
    def check_fields(fields):
        """
        \brief (Internal use)
        \param Test whether the fields parameter of the constructor is well-formed
        \return True iif everything is fine, False otherwise
        """
        for field in fields:
            if not isinstance(field, MetadataField):
                return False
        return True

    def __init__(fields):
        """
        \brief Constructor
        \param fields The set of Metafields involved in the key.
        """
        if check_fields(fields) == False:
            raise TypeError("Invalid fields parameter: %r" % fields)
        self = fields

    @returns(bool)
    def is_composite_key():
        """
        \brief Test whether a key is made of more that one field (composite key)
        \return True if the key is composite, False otherwise
        """
        return len(list(self)) > 0

    @returns(StringTypes)
    def get_type():
        if self.is_composite_key():
            raise ValueError("get_type cannot be called for a composite key")
        return list(self)[0].type

    @returns(StringTypes)
    def get_type():
        if self.is_composite_key():
            raise ValueError("get_type cannot be called for a composite key")
        return list(self)[0].field_name

class Keys(set):
    """
    Implements a set of keys for a table.
    """

    @staticmethod
    @returns(bool)
    def check_keys(keys):
        """
        \brief (Internal use)
        \param Test whether the keys parameter of the constructor is well-formed
        \return True iif everything is fine, False otherwise
        """
        if not isinstance(keys, (frozenset, set)):
            return False
        for key in keys:
            if not isinstance(key, Key):
                return False
        return True

    def __init__(keys):
        """
        \brief Constructor
        \param keys A set of Key instances
        """
        if check_fields(fields) == False:
            raise TypeError("Invalid fields parameter: %r" % fields)
        self = fields

    def get_field_names(self):
        """
        \brief Returns a set of fields making up one key. If multiple possible
        keys exist, a unique one is returned, having a minimal size.
        """
        return min(self, key=len)
            
            
