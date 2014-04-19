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

class Key(object):
    """
    Implements a key for a table.
    A key is a set of (eventually one) Fields.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, fields, local = False):
        """
        Constructor.
        Args:
            fields: The set of Field instances involved in the Key.
        """
        Key.check_fields(fields)
        self._fields = frozenset(fields)
        self._local = local

    @classmethod
    def from_dict(cls, key_dict, all_fields_dict):
        """
        Args:
            key_dict (dict):

            all_fields_dict (dict) : dictionary mapping field names (string) to
            their corresponding Field.

        See also:
            manifold.core.field
        """
        field_names = key_dict.get('field_names', [])
        fields = [all_fields_dict[field_name] for field_name in field_names]
        return Key(fields, key_dict.get('local', False))

    def to_dict(self):
        return {
            'field_names': list(self.get_field_names()),
            'local': self.is_local()
        }

    #-----------------------------------------------------------------------
    # Internal methods
    #-----------------------------------------------------------------------

    def __iter__(self):
        return iter(self._fields)


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def set_local(self):
        self._local = True

    def is_local(self):
        return self._local

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    @returns(bool)
    def is_composite(self):
        """
        Test whether a key is made of more that one field (composite key)
        Returns:
            True if the key is composite, False otherwise
        """
        return len(list(self)) > 1

    #---------------------------------------------------------------------------
    # Static methods
    #---------------------------------------------------------------------------

    @staticmethod
    def check_fields(fields):
        """
        (Internal use)
        Test whether the fields parameter of the constructor is well-formed
        """
        for field in fields:
            if not isinstance(field, Field):
                raise TypeError("field = %r is of type %r (Field expected)" % (field, type(field)))

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(frozenset)
    def get_fields(self):
        return set(self._fields)

    @returns(Field)
    def get_field(self):
        if self.is_composite():
            raise ValueError("get_field cannot be called for a composite key")
        return iter(self._fields).next()

    @returns(StringTypes)
    def get_name(self):
        Log.deprecated('get_field_name')
        return self.get_field_name()

    @returns(StringTypes)
    def get_field_name(self):
        return self.get_field().get_name()

    @returns(set)
    def get_field_names(self):
        return Fields([x.get_name() for x in self._fields])

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
        return set([field.get_type() for field in self._fields])

    @returns(StringTypes)
    def __str__(self):
        local = 'LOCAL ' if self.is_local() else ''
        return "%sKEY(%s)" % (local, ", ".join(["%s" % field for field in self._fields]))

    @returns(StringTypes)
    def __repr__(self):
        local = 'LOCAL ' if self.is_local() else ''
        return "%sKEY(%s)" % (local, ", ".join(["%s" % field for field in self._fields])) 
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
