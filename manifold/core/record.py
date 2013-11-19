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
        self._last = False


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_dict(self):
        return self._dict

    def to_dict(self):
        dic = {}
        for k, v in self._dict.iteritems():
            if isinstance(v, Record):
                dic[k] = v.to_dict()
            elif isinstance(v, Records):
                dic[k] = v.to_list()
            else:
                dic[k] = v
        return dic

    def get_last(self):
        return self._last

    def set_last(self, value):
        self._last = value

    def is_last(self):
        return self._last


    #--------------------------------------------------------------------------- 
    # Internal methods
    #--------------------------------------------------------------------------- 

    def __repr__(self):
        content = [
            ("%r" % self._dict) if self._dict else '',
            'LAST' if self._last else ''
        ]
        return "<Record %s>" % ' '.join(content)

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __iter__(self): 
        return self._dict.itervalues()


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



class LastRecord(Record):
    def __init__(self, *args, **kwargs):
        Record.__init__(self, *args, **kwargs)
        self._last = True

class Records(list):
    """
    A list of records
    """

    def __init__(self, itr = None): 
        if itr:
            list.__init__(self, [Record(x) for x in itr])
        else:
            list.__init__(self)

    def to_list(self):
        return [record.to_dict() for record in self]
