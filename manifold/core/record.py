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

import collections
from types                         import StringTypes
from manifold.util.log             import Log
from manifold.util.type            import returns, accepts

FIELD_SEPARATOR = '.'

class Record(collections.MutableMapping):
    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        self._annotations = dict()

    def set_annotation(self, key, value):
        self._annotations[key] = value

    def get_annotation(self, key = None):
        if key:
            return self._annotations.get(key)
        return self._annotations

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __repr__(self):
        return self.store.__repr__()
    def __str__(self):
        return self.store.__str__()

#old#     @classmethod
#old#     def get_value(self, record, key):
#old#         """
#old#         Args:
#old#             record: A Record instance.
#old#             key: A String instance (field name), or a set of String instances
#old#                 (field names)
#old#         Returns:
#old#             If key is a String,  return the corresponding value.
#old#             If key is a set, return a tuple of corresponding value.
#old#         """
#old#         if isinstance(key, StringTypes):
#old#             return record[key]
#old#         else:
#old#             return tuple(map(lambda x: record[x], key))
    def get_value(self, fields):
        """
        Args:
            fields: A String instance (field name), or a set of String instances
                (field names) # XXX tuple !!
        Returns:
            If fields is a String,  return the corresponding value.
            If fields is a set, return a tuple of corresponding value.

        Raises:
            KeyError if at least one of the fields is not found
        """
        record = self

        if not fields:
            return None

        if isinstance(fields, StringTypes):
            if FIELD_SEPARATOR in fields:
                key, _, subkey = fields.partition(FIELD_SEPARATOR)
                if not key in record:
                    return None
                if isinstance(record[key], Records):
                    return [subrecord.get_value(sukey) for subrecord in record[key]]
                elif isinstance(record[key], Record):
                    return record[key].get_value(subkey)
                else:
                    raise Exception, "Unknown fields"
            else:
                return record[fields]
        else: # We have an iterable

            first_field = iter(fields).next()

            # XXX see. get_map_entries
            if len(fields) == 1:
                return self.get_value(first_field)

            # In the general case, we have multiple keys
            # HYPOTHESE: Let's assume all the subfields are from the same
            # table. This is the case when we retrieve the key of a 1..N
            # relation.
            
            if FIELD_SEPARATOR in first_field:
                subfields = list()
                for field in fields:
                    key, _, subkey = field.partition(FIELD_SEPARATOR)
                    subfields.append(subkey)

                if not key in record:
                    return None
                if not record[key]:
                    return None
                if isinstance(record[key], (Records, list)):
                    return [subrecord.get_value(subfields) for subrecord in record[key]]
                elif isinstance(record[key], Record):
                    return record[key].get_value(subfields)
                else:
                    raise Exception, "Unknown fields"
            
            else:
                return tuple(map(lambda x: self.get_value(x), fields))


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

    def is_empty(self):
        return not self

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
