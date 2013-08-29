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

class Record(object):

    @classmethod
    def get_value(self, record, key):
        """
        Args:
            record: A Record instance.
            key: A String instance (field name), or a set of String instances
                (field names)
        Returns:
            If key is a String,  return the corresponding value.
            If key is a set, return a tuple of corresponding value.
        """
        if isinstance(key, StringTypes):
            return record[key]
        else:
            return tuple(map(lambda x: record[x], key))

    @classmethod
    @returns(dict)
    def from_key_value(self, key, value):
        if isinstance(key, StringTypes):
            return { key: value }
        else:
            return dict(izip(key, value))

    @classmethod
    @returns(bool)
    def has_fields(self, record, fields):
        """
        Test whether a Record carries a set of fields.
        Args:
            record: A Record instance.
            fields: A String instance (field name) or
                    a set of String instances (field names)
        Returns:
            True iif record carries this set of fields.
        """
        if isinstance(fields, StringTypes):
            return fields in record
        else:
            return fields <= set(record.keys())
   
    @classmethod
    @returns(bool)
    def is_empty_record(self, record, keys):
        for key in keys:
            if record[key]: return False
        return True


