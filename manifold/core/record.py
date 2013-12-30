#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Record
# A Record is a Packet transporting value resulting of a Query.
# A Record can be seen as a python dictionnary where
# - each key corresponds to a field name
# - each value corresponds to the corresponding field value.
# 
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                  import StringTypes

from manifold.core.packet   import Packet
from manifold.util.log      import Log
from manifold.util.type     import returns, accepts

class Record(Packet):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        Packet.__init__(self, Packet.PROTOCOL_RECORD, last = False, **kwargs)
        if args:
            print "args", args
            if len(args) == 1 and isinstance(args[0], (Record, dict)):
                self._record = dict(args[0])
            else:
                raise Exception, "Bad initializer for Record"
        else:
            self._record = None

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
        return self._record

    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dict representation of this Record.
        """
        dic = dict() 
        for k, v in self._record.iteritems():
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
        return "<Record %s>" % ' '.join([("%s" % self._record) if self._record else ''])

    def __getitem__(self, key, **kwargs):
        """
        Extract from this Record a field value.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
        Returns:
            The corresponding value. 
        """
        return dict.__getitem__(self._record, key, **kwargs)

    def __setitem__(self, key, value, **kwargs):
        """
        Set the value corresponding to a given key.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
            value: The value that must be mapped with this key.
        """
        return dict.__setitem__(self._record, key, value, **kwargs)

    def __iter__(self): 
        return dict.__iter__(self._record)

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
            return self._record[key]
        else:
            return tuple(map(lambda x: self._record[x], key))

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
            return fields in self._record
        else:
            return fields <= set(self._record.keys())
   
    @returns(bool)
    def has_empty_fields(self, keys):
        for key in keys:
            if self._record[key]: return False
        return True

    def pop(self, key):
        """
        """
        return dict.pop(self._record, key)

    def items(self):
        return dict.items(self._record)

    def keys(self):
        return dict.keys(self._record)

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
