#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Record and Records classes.
#
# A Record is a Packet transporting value resulting of a Query.
# A Record behaves like a python dictionnary where:
# - each key corresponds to a field name
# - each value corresponds to the corresponding field value.
# 
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

#import uuid
from types                  import GeneratorType, StringTypes

from manifold.core.fields   import Fields, FIELD_SEPARATOR
from manifold.core.packet   import Packet
from manifold.util.log      import Log
from manifold.util.type     import returns, accepts

#-------------------------------------------------------------------------------
# Record class
#-------------------------------------------------------------------------------

class Record(Packet):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        if not 'last' in kwargs:
            kwargs['last'] = False
        Packet.__init__(self, Packet.PROTOCOL_RECORD, **kwargs)
        if args:
            if len(args) == 1:
                self._record = dict(args[0])
            else:
                raise Exception, "Bad initializer for Record"
        else:
            self._record = None
        #self._parent_uuid = None
        #self._uuid = None

    @staticmethod
    def from_dict(dic):
        record = Record()
        record.set_dict(dic)
        return record

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

    def set_dict(self, dic):
        self._record = dic

    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dict representation of this Record.
        """
        dic = dict() 
        if self._record:
            try:
                for k, v in self._record.iteritems():
                    if isinstance(v, Record):
                        dic[k] = v.to_dict()
                    elif isinstance(v, Records):
                        dic[k] = v.to_list()
                    else:
                        dic[k] = v
            except Exception, e:
                print "EEEEEE", e
                import pdb; pdb.set_trace()
        return dic

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif this Record is the last one of a list
            of Records corresponding to a given Query.
        """
        return self._record is None

    #--------------------------------------------------------------------------- 
    # Internal methods
    #--------------------------------------------------------------------------- 

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%s' representation of this Record.
        """
        return "<Record %s%s>" % (
            ' '.join([("%s" % self._record) if self._record else '']),
            ' LAST' if self.is_last() else ''
        )

    def __getitem__(self, key, **kwargs):
        """
        Extract from this Record a field value.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
        Returns:
            The corresponding value. 
        """
        if not self._record:
            raise Exception, "Empty record"
        return dict.__getitem__(self._record, key, **kwargs)

    def __setitem__(self, key, value, **kwargs):
        """
        Set the value corresponding to a given key.
        Args:
            key: A String instance corresponding to a field name
                of this Record.
            value: The value that must be mapped with this key.
        """
        if not self._record:
            self._record = dict()
        return dict.__setitem__(self._record, key, value, **kwargs)

    def __iter__(self): 
        """
        Returns:
            A dictionary-keyiterator allowing to iterate on fields
            of this Record.
        """
        return dict.__iter__(self._record)

    def get(self, value, default=None):
        return self._record.get(value, default)

    #--------------------------------------------------------------------------- 
    # Class methods
    #--------------------------------------------------------------------------- 

    @classmethod
    @returns(dict)
    def from_key_value(self, key, value):
        if isinstance(key, StringTypes):
            return {key : value}
        else:
            return Record(izip(key, value))

    #--------------------------------------------------------------------------- 
    # Methods
    #--------------------------------------------------------------------------- 

    def get_map_entries(self, fields):
        """
        Internal use for left_join
        """
        if isinstance(fields, Fields):
            assert len(fields) == 1
            fields = iter(fields).next()

        # fields is now a string
        field, _, subfield = fields.partition(FIELD_SEPARATOR)

        if not subfield:
            if field in self._record:
                return [(self._record[field], self._record)]
            else:
                return []
        else:
            ret = list()
            for record in self._record[field]:
                tuple_list = record.get_map_entries(subfield)
                ret.extend(tuple_list)
            return ret

    def get_value(self, fields):
        """
        Args:
            fields: A String instance (field name), or a set of String instances
                (field names) # XXX tuple !!
        Returns:
            If fields is a String,  return the corresponding value.
            If fields is a set, return a tuple of corresponding value.
        """
        assert isinstance(fields, (StringTypes, tuple))

        if isinstance(fields, StringTypes):
            return self._record[fields]
        else:
            # XXX We expect no "." inside XXX 
            return tuple(map(lambda x: self.get_value(x), fields))


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
            # SHOULD BE DEPRECATED SOON since we are only using the Fields()
            # class now...
            return fields in self._record

        fields, map_method_subfields, _, _ = fields.split_subfields()
        if not set(fields) <= set(self._record.keys()):
            return False

        for method, subfields in map_method_subfields.items():
            # XXX 1..1 not taken into account here
            for record in self._record[method]:
                if not record.has_fields(subfields):
                    return False
        return True

#DEPRECATED|        # self._record.keys() should have type Fields, otherwise comparison
#DEPRECATED|        # fails without casting to set
#DEPRECATED|        return set(fields) <= set(self._record.keys())

    @returns(bool)
    def has_empty_fields(self, keys):
        for key in keys:
            if self._record[key]: return False
        return True

    def pop(self, key):
        return dict.pop(self._record, key)

    def items(self):
        return dict.items(self._record) if self._record else list()

    @returns(list)
    def keys(self):
        """
        Returns:
            A list of String where each String correspond to a field
            name of this Record.
        """
        return dict.keys(self._record) if self._record else list()

    def get_fields(self):
        return Fields(self.keys())

    def update(self, other_record):
        return dict.update(self._record, other_record)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this QUERY Packet.
        """
        return "<Packet.%s %s>" % (
            Packet.get_protocol_name(self.get_protocol()),
            self.to_dict()
        )

#DEPRECATED|    def get_uuid(self):
#DEPRECATED|        if not self._uuid:
#DEPRECATED|            self._uuid = str(uuid.uuid4())
#DEPRECATED|        return self._uuid
#DEPRECATED|
#DEPRECATED|    def set_parent_uuid(self, uuid):
#DEPRECATED|        self._parent_uuid = uuid


#-------------------------------------------------------------------------------
# Records class
#-------------------------------------------------------------------------------

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
    def to_dict_list(self):
        """
        Returns:
            The list of Record instance corresponding to this
            Records instance.
        """
        return [record.to_dict() for record in self]

    to_list = to_dict_list

    def get_one(self):
        return self[0]

    def get_fields(self):
        return self.get_one().get_fields()

    def add_record(self, record):
        self.append(record)

    def add_records(self, records):
        self.extend(records)
