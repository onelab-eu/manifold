#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Record
# Records transport values fetched by a Query.
# A Record can be seen as a python dictionnary where
# - each key corresponds to a field name
# - each value corresponds to the corresponding field value.
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# TODO:
# Currently, records are dictionary, but we will need a special class for typed
# operations, and faster processing. Hence, consider the current code as a
# transition towards this new class.

from types                  import StringTypes

from manifold.core.packet   import Packet
from manifold.util.log      import Log
from manifold.util.type     import returns, accepts

class Record(Packet):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        Packet.__init__(self, Packet.TYPE_RECORD)
        self._dict = dict(*args, **kwargs)

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(dict)
    def get_dict(self):
        """
        Returns:
            The dict nested in this Record. Note that most of time
            you should use to_dict() method instead.
        """
        return self._dict

    def to_dict(self):
        """
        Returns:
            The dict representation of this Record.
        """
        dic = {}
        for k, v in self._dict.iteritems():
            if isinstance(v, Record):
                dic[k] = v.to_dict()
            elif isinstance(v, Records):
                dic[k] = v.to_list()
            else:
                dic[k] = v
        return dic

    @returns(bool)
    def is_last(self):
        """
        (This method is overwritten in LastRecord)
        Returns:
            True iif this Record is the last one of a list
            of Records corresponding to a given Query.
        """
        return False 

    #--------------------------------------------------------------------------- 
    # Internal methods
    #--------------------------------------------------------------------------- 

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%s' representation of this Record.
        """
        return "<Record %s>" % ' '.join([("%s" % self._dict) if self._dict else ''])

    def __getitem__(self, key, **kwargs):
        """
        Extract from this Record a field value.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
        Returns:
            The corresponding value. 
        """
        return dict.__getitem__(self._dict, key, **kwargs)

    def __setitem__(self, key, value, **kwargs):
        """
        Set the value corresponding to a given key.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
            value: The value that must be mapped with this key.
        """
        return dict.__setitem__(self._dict, key, value, **kwargs)

    def __iter__(self): 
        return dict.__iter__(self._dict)

    #--------------------------------------------------------------------------- 
    # Class methods
    #--------------------------------------------------------------------------- 

    @classmethod
    @returns(dict)
    def from_key_value(self, key, value):
        if isinstance(key, StringTypes):
            return { key: value }
        else:
            return Record(izip(key, value))

    #--------------------------------------------------------------------------- 
    # Methods
    #--------------------------------------------------------------------------- 

    def get_value(self, key):
        """
        Args:
            key: A String instance (field name), or a set of String instances
                (field names)
        Returns:
            If key is a String,  return the corresponding value.
            If key is a set, return a tuple of corresponding value.
        """
        if isinstance(key, StringTypes):
            return self._dict[key]
        else:
            return tuple(map(lambda x: self._dict[x], key))

    @returns(bool)
    def has_fields(self, fields):
        """
        Test whether a Record carries a set of fields.
        Args:
            fields: A String instance (field name) or
                    a set of String instances (field names)
        Returns:
            True iif record carries this set of fields.
        """
        if isinstance(fields, StringTypes):
            return fields in self._dict
        else:
            return fields <= set(self._dict.keys())
   
    @returns(bool)
    def is_empty(self, keys):
        for key in keys:
            if self._dict[key]: return False
        return True

    def pop(self, key):
        """
        """
        return dict.pop(self._dict, key)

    def items(self):
        return dict.items(self._dict)

    def keys(self):
        return dict.keys(self._dict)

class LastRecord(Record):
    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        super(LastRecord, self).__init__(*args, **kwargs)

    @returns(bool)
    def is_last(self):
        """
        (This method is overwritten in LastRecord)
        Returns:
            True iif this Record is the last one of a list
            of Records corresponding to a given Query.
        """
        return True 

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%s' representation of this Record.
        """
        return 'LAST'

class Records(list):
    """
    A Records instance transport a list of Record instances.
    """

    def __init__(self, itr = None): 
        """
        Constructor.
        Args:
            itr: An Iterable instance containing instances that
                can be casted into a Record (namely dict or
                Record instance). For example, itr may be
                a list of dict (having the same keys).
        """
        if itr:
            list.__init__(self, [Record(x) for x in itr])
        else:
            list.__init__(self)

    @returns(list)
    def to_list(self):
        """
        Returns:
            The list of Record instance corresponding to this
            Records instance.
        """
        return [record.to_dict() for record in self]
