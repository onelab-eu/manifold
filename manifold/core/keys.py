#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Key:
#   Represent a key of a Table instance
#   A key is a set of fields (eventually one).
#   \sa manifold.core.table.py
#   \sa manifold.core.field.py
# Class Keys:
#   A Table instance carries a Keys instance, e.g a set of Key instances.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import copy
from types                  import StringTypes
from manifold.core.field    import Field, merge_fields
from manifold.core.key      import Key
from manifold.util.type     import returns, accepts
from manifold.util.log      import Log

class Keys(set):
    """
    Implements a set of keys for a table.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, keys = set()):
        """
        Constructor
        Args:
            keys: A set/frozenset/list of Key instances
        """
        Keys.check_keys(keys)
        set.__init__(self, set(keys))

    @classmethod
    def from_dict_list(cls, key_dict_list, all_fields_dict):
        return Keys([Key.from_dict(key_dict, all_fields_dict) for key_dict in key_dict_list])

    def to_dict_list(self):
        return [key.to_dict() for key in self]

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def is_local(self):
        return all(key.is_local() for key in self)

    #---------------------------------------------------------------------------
    # Static methods
    #---------------------------------------------------------------------------

    @staticmethod
    def check_keys(keys):
        """
        (Internal use)
        Test whether the keys parameter of the constructor is well-formed
        Args:
            keys: The keys parameter passed to __init__
        """
        if not isinstance(keys, (frozenset, set, list)):
            raise TypeError("keys = %r is of type %r (set or frozenset expected)" % (keys, type(keys)))
        for key in keys:
            if not isinstance(key, Key):
                raise TypeError("key = %r is of type %r (Key expected)" % (key, type(key)))

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Keys instance.
        """
        return "Keys(%s)" % (", ".join(["%s" % key for key in self]))

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Keys instance.
        """
        return str(self) 

    @returns(bool)
    def has_field(self, field):
        """
        Test whether a Field is involved in at least one Key
        of this Keys instance.
        Args:
            field: A Field instance.
        Returns:
            True iif 'field' is involved in this Keys instance.
        """
        for key in self:
            if field in key:
                return True
        return False

    @returns(Key)
    def one(self):
        """
        Returns:
            The first Key instance contained in this Keys instance.
        """
        if len(self) != 1:
            Log.warning("one() called with more than one key (self = %s)" % self)
        # XXX Note we might need to prevent multiple key cases
        return iter(self).next()

#    def get_field_names(self):
#        """
#        \brief Returns a set of fields making up one key. If multiple possible
#        keys exist, a unique one is returned, having a minimal size.
#        """
#        return min(self, key=len)

    def add(self, key2):
        """
        Add a Key instance into this Keys instance.
        Remark:
            If KEY([const foo bar, ...]) is added and KEY([foo bar, ...]) is already in self, it is ignored.
            If KEY([foo bar, ...]) is added and KEY([const foo bar, ...]) is already in self, KEY(const foo bar) is replaced.
        Args:
            key2: A Key instance.
        """
        assert isinstance(key2, Key)
        new_fields = set()
        for key1 in self:
            if key1.get_field_names() == key2.get_field_names():
                Log.warning("Key collision is not working properly. Missing == and &")
                break
                # key1 and key2 collide, build a new key which merge key1 and key2
                for field1 in key1:
                    field_name = field1.get_name()
                    field2 = key2.get_field(field_name) # This should never raise an exception 
                    new_fields.add(field1 | field2)

                # Replace key1 with the newly crafted key.
                self.remove(key1)
                super(Keys, self).add(Key(new_fields)) # XXX Missing local
                return

        super(Keys, self).add(key2)

    #@returns(Keys)
    def __ior__(self, keys):
        """
        Add in self several Key instances.
        Args:
            keys: A set of Key instances or a Keys instance/
        Returns:
            The resulting Keys instance.
        """
        for key in keys:
            self.add(key)
        return self

    def __hash__(self):
        return hash(frozenset(self))

#@returns(set)
#def merge_keys(keys1, keys2):
#    """
#    Compute the intersection of two sets of Key instances.
#    Args:
#        keys1: A set of Key instances.
#        keys2: A set of Key instances.
#    Returns:
#        The corresponding intersection (set of Keys)
#    """
#    Log.tmp("keys1 = ",keys1)
#    Log.tmp("keys2 = ",keys2)
#    return keys2
#
#    s1 = set()
#    for key in keys1: 
#        if not key.is_local():
#            s1.add(key.get_fields())
#
#    s1 = Fields(set([key.get_fields() for key in keys1 if not key.is_local()]))
#    if s1:
#        s2 = Fields(set([key.get_fields() for key in keys2 if not key.is_local()]))
#        Log.tmp(s1)
#        Log.tmp(s2)
#        s = merge_fields(s1, s2)
#    else:
#        s = set()
#
#    keys = set()
#    for fields in s:
#        key = Key(fields, local = False)
#        keys.add(key)
#    return keys
