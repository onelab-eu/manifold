#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Key:
#   Represent a key of a Table instance
#   A key is a set of (eventually one) Fields.
#   \sa manifold.core.table.py
#   \sa manifold.core.field.py
# Class Keys:
#   A Table instance carries a Keys instance, e.g a set of Key instances.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.util.type      import returns, accepts
from manifold.core.field     import Field

class Key(frozenset):
    """
    Implements a key for a table.
    A key is a set of (eventually one) Fields.
    """

    @staticmethod
    def check_fields(fields):
        """
        \brief (Internal use)
               Test whether the fields parameter of the constructor is well-formed
        \param fields The fields parameter passed to __init__
        """
        for field in fields:
            if not isinstance(field, Field):
                raise TypeError("field = %r is of type %r (Field expected)" % (field, type(field)))

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
        return len(list(self)) > 1

    def get_field(self):
        if self.is_composite():
            raise ValueError("get_field cannot be called for a composite key")
        return list(self)[0]

    @returns(str)
    def get_name(self):
        return self.get_field().get_name()

    @returns(str)
    def get_type(self):
        return self.get_field().get_type()

    @returns(str)
    def __str__(self):
        return "KEY(%s)" % (", ".join(["%s" % field for field in self]))

    @returns(str)
    def __repr__(self):
        return "KEY(%s)" % (", ".join(["%s" % field for field in self]))

# DO NOT UNCOMMENT
    @returns(bool)
    def __eq__(self, x):
#        return set([f.get_name() for f in self]) == set(f.get_name() for f in x)
        return self.__hash__() == x.__hash__()
#
#    def __hash__(self):
#        return hash(tuple([f.get_name() for f in self]))

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
        """
        if not isinstance(keys, (frozenset, set, list)):
            raise TypeError("keys = %r is of type %r (set or frozenset expected)" % (keys, type(keys)))
        for key in keys:
            if not isinstance(key, Key):
                raise TypeError("key = %r is of type %r (Key expected)" % (key, type(key)))

    def __init__(self, keys = set()):
        """
        \brief Constructor
        \param keys A set/frozenset/list of Key instances
        """
        Keys.check_keys(keys)
        set.__init__(set(keys))

    @returns(str)
    def __str__(self):
        return "{%s}" % (", ".join(["%s" % key for key in self]))

    @returns(str)
    def __repr__(self):
        return "{%s}" % (", ".join(["%r" % key for key in self]))


#    def get_field_names(self):
#        """
#        \brief Returns a set of fields making up one key. If multiple possible
#        keys exist, a unique one is returned, having a minimal size.
#        """
#        return min(self, key=len)
