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

import copy
from types                  import StringTypes
from manifold.core.field    import Field
from manifold.core.fields   import Fields
from manifold.util.type     import returns, accepts
from manifold.util.log      import Log

class Key(frozenset):
    """
    Implements a key for a table.
    A key is a set of (eventually one) Fields.
    """

    @staticmethod
    def check_fields(fields):
        """
        (Internal use)
        Test whether the fields parameter of the constructor is well-formed
        """
        for field in fields:
            if not isinstance(field, Field):
                raise TypeError("field = %r is of type %r (Field expected)" % (field, type(field)))

    def __init__(self, fields):
        """
        Constructor.
        Args:
            fields: The set of Field instances involved in the Key.
        """
        Key.check_fields(fields)
        frozenset.__init__(fields)

    @returns(bool)
    def is_composite(self):
        """
        Test whether a key is made of more that one field (composite key)
        Returns:
            True if the key is composite, False otherwise
        """
        return len(list(self)) > 1

    @returns(Field)
    def get_field(self):
        if self.is_composite():
            raise ValueError("get_field cannot be called for a composite key")
        return list(self)[0]

    @returns(StringTypes)
    def get_name(self):
        Log.deprecated('get_field_name')
        return self.get_field_name()

    @returns(StringTypes)
    def get_field_name(self):
        return self.get_field().get_name()

    @returns(set)
    def get_field_names(self):
        return Fields([x.get_name() for x in self])

    @returns(set)
    def get_names(self):
        Log.deprecated('get_field_names')
        return self.get_field_names()

    def get_minimal_names(self):
        return self.get_field_names() if self.is_composite() else self.get_name()
        
    @returns(StringTypes)
    def get_type(self):
        Log.deprecated('get_field_type')
        return self.get_field_type()

    @returns(StringTypes)
    def get_field_type(self):
        return self.get_field().get_type()

    def get_field_types(self):
        return set([field.get_type() for field in self])

    @returns(StringTypes)
    def __str__(self):
        return "KEY(%s)" % (", ".join(["%s" % field for field in self]))

    @returns(StringTypes)
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

    @returns(bool)
    def is_local(self):
        """
        Returns:
            True iif a Key involves at least one local Field/
        """
        for field in self:
            if field.is_local(): return True
        return False

class Keys(set):
    """
    Implements a set of keys for a table.
    """

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

    def __init__(self, keys = set()):
        """
        Constructor
        Args:
            keys: A set/frozenset/list of Key instances
        """
        Keys.check_keys(keys)
        set.__init__(self, set(keys))
        self._local = False

    def get_local(self):
        return self._local

    def set_local(self, table):
        print "KEYS SET LOCAL", self, table
        self._local = table

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
        assert len(self) == 1, "Cannot call one() when not exactly 1 keys (self = %s)" % self
        # XXX Note we might need to prevent multiple key cases
        return iter(self).next()

#    def get_field_names(self):
#        """
#        \brief Returns a set of fields making up one key. If multiple possible
#        keys exist, a unique one is returned, having a minimal size.
#        """
#        return min(self, key=len)

    def __hash__(self):
        return hash(frozenset(self))

    #@returns(Key)
    def __or__(self, key2):
        assert len(key1) == len(key2)

        key1 = self
        fields = set()

        for field1 in key1:
            merged = False

            for field2 in key2:
                # Merge matching fields 
                try:
                    fields.add(field1 | field2)
                    merged = True
                    break
                except ValueError:
                    continue

            if not merged:
                # No matching Field, the both Key cannot be merged 
                raise ValueError("Cannot %s | %s" % (self, key2))

        assert len(fields) == len(key1)
        return Key(fields)

