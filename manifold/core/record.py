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

from types                         import StringTypes
from manifold.util.log             import Log
from manifold.util.type            import returns, accepts

class Record(dict):

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
            return self[key]
        else:
            return tuple(map(lambda x: self[x], key))

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
            return fields in self
        else:
            return fields <= set(self.keys())
   
    @returns(bool)
    def is_empty(self, keys):
        for key in keys:
            if self[key]: return False
        return True

    def to_dict(self):
        dic = {}
        for k, v in self.iteritems():
            if isinstance(v, Record):
                dic[k] = v.to_dict()
            elif isinstance(v, Records):
                dic[k] = v.to_list()
            else:
                dic[k] = v
        return dic

    def is_last(self):
        return False


class LastRecord(Record):
    def is_last(self):
        return True

class Records(list):
    """
    A list of records
    """

    def __init__(self, itr): 
        list.__init__(self, [Record(x) for x in itr])

    def to_list(self):
        return [record.to_dict() for record in self]
