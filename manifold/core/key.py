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
from types                      import StringTypes

from manifold.core.field        import Field
from manifold.core.field_names  import FieldNames
from manifold.util.type         import returns, accepts
from manifold.util.log          import Log

class Key(object):
    """
    Implements a key for a table.
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
        for field in fields:
            assert isinstance(field, Field)
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
        field_names = key_dict.get("field_names", list())
        fields = [all_fields_dict[field_name] for field_name in field_names]
        return Key(fields, key_dict.get("local", False))

    @returns(dict)
    def to_dict(self):
        return {
            "field_names" : list(self.get_field_names()),
            "local"       : self.is_local()
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

    @returns(bool)
    def is_local(self):
        return self._local

    @returns(bool)
    def is_empty(self):
        return not self._fields

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
        return len(list(self._fields)) > 1

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
        """
        Returns:
            A frozenset of Field instances.
        """
        return frozenset(self._fields)

    @returns(Field)
    def get_field(self, field_name = None):
        """
        Retrieve a Field involved in this Key.
        Args:
            field_name: A String corresponding to the name of Field contained in this Key.
                If the Key is made of only one Field, you can pass None.
        Raises:
            ValueError: if field_name is omitted and self is composite.
            KeyError: if self does not contains such a Field.
            RuntimeError: if self is empty.
        Returns:
            The corresponding Field instance.
        """
        assert not field_name or isinstance(field_name, StringTypes)
        if self.is_composite() and not field_name:
            raise ValueError("get_field cannot be called for a composite key without precising the field name")
        if field_name:
            # field_name is precised, explore this Key
            for field in self:
                if field.get_name() == field_name:
                    return field
            raise KeyError("%s does not contain a field with name = %s" % (self, field_name))
        elif not self._fields:
            # This Key is empty, nothing to return
            #return None
            raise RuntimeError("%s is empty, you cannot call get_field on such a Key" % self)
        else:
            # Returns the unique Field contained in this Key
            return iter(self._fields).next()

#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def get_name(self):
#DEPRECATED|        Log.deprecated('get_field_name')
#DEPRECATED|        return self.get_field_name()

    @returns(StringTypes)
    def get_field_name(self):
        field = self.get_field()
        if not field:
            return None
        return field.get_name()

    @returns(FieldNames)
    def get_field_names(self):
        return FieldNames([field.get_name() for field in self._fields])

#DEPRECATED|    @returns(set)
#DEPRECATED|    def get_names(self):
#DEPRECATED|        Log.deprecated('get_field_names')
#DEPRECATED|        return self.get_field_names()

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

    def __eq__(self, x):
        return self.get_fields() == x.get_fields()

    def __hash__(self):
        return self._fields.__hash__()

#DEPRECATED|# DO NOT UNCOMMENT
#DEPRECATED|    @returns(bool)
#DEPRECATED|    def __eq__(self, x):
#DEPRECATED|#        return set([f.get_name() for f in self]) == set(f.get_name() for f in x)
#DEPRECATED|        return self.__hash__() == x.__hash__()
#DEPRECATED|#
#DEPRECATED|#    def __hash__(self):
#DEPRECATED|#        return hash(tuple([f.get_name() for f in self]))
            
