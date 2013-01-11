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

class Key(frozenset):
    """
    Implements a key for a table.
    A key is a set of (eventually one) MetadataFields.
    """

    @staticmethod
    def check_fields(fields):
        """
        \brief (Internal use)
               Test whether the fields parameter of the constructor is well-formed
        \param fields The fields parameter passed to __init__
        """
        for field in fields:
            if not isinstance(field, MetadataField):
                raise TypeError("field = %r is of type %r (MetadataField expected)" % (field, type(field)))

    def __init__(self, fields):
        """
        \brief Constructor
        \param fields The set of Metafields involved in the key.
        """
        Key.check_fields(fields)
        frozenset.__init__(fields)

    @returns(bool)
    def is_composite(self):
        """
        \brief Test whether a key is made of more that one field (composite key)
        \return True if the key is composite, False otherwise
        """
        return len(list(self)) > 0

    @returns(str)
    def get_type(self):
        if self.is_composite():
            raise ValueError("get_type cannot be called for a composite key")
        return list(self)[0].get_type()

    @returns(str)
    def get_name(self):
        if self.is_composite():
            raise ValueError("get_name cannot be called for a composite key")
        return list(self)[0].get_name()

    @returns(str)
    def __str__(self):
        return "KEY(%s)" % (", ".join(["%r" % field for field in self]))

    @returns(str)
    def __repr__(self):
        return "KEY(%s)" % (", ".join(["%r" % field for field in self]))

class Keys(set):
    """
    Implements a set of keys for a table.
    """

    @staticmethod
    def check_keys(keys):
        """
        \brief (Internal use)
               Test whether the keys parameter of the constructor is well-formed
        \param keys The keys parameter passed to __init__
        \return True iif everything is fine, False otherwise
        """
        if not isinstance(keys, (frozenset, set, list)):
            raise TypeError("keys = %r is of type %r (set or frozenset expected)" % (keys, type(keys)))
        for key in keys:
            if not isinstance(key, Key):
                raise TypeError("key = %r is of type %r (Key expected)" % (key, type(key)))

    def __init__(self, keys = set()):
        """
        \brief Constructor
        \param keys A set of Key instances
        """
        Keys.check_keys(keys)
        set.__init__(set(keys))

    @returns(str)
    def __str__(self):
        return "{%s}" % (", ".join(["%s" % (key) for key in self]))

    @returns(str)
    def __repr__(self):
        return "{%s}" % (", ".join(["%r" % (key) for key in self]))


#    def get_field_names(self):
#        """
#        \brief Returns a set of fields making up one key. If multiple possible
#        keys exist, a unique one is returned, having a minimal size.
#        """
#        return min(self, key=len)
            
            
