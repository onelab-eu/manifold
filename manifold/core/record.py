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
from types                      import GeneratorType, StringTypes

from manifold.core.field_names  import FieldNames, FIELD_SEPARATOR
from manifold.core.packet       import Packet
from manifold.util.log          import Log
from manifold.util.type         import returns, accepts

#-------------------------------------------------------------------------------
# Record class
#-------------------------------------------------------------------------------

class Unspecified(object):
    pass

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
            # We need None to test whether the record is empty
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

    def __getitem__(self, key):
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
        return self.get(key)
#        return dict.__getitem__(self._record, key, **kwargs)

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
        if self._record is None:
            return dict.__iter__({})
        return dict.__iter__(self._record)

#    def get(self, value, default=None):
#        return self._record.get(value, default)

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

    # XXX This should disappear when we have a nice get_value
    @returns(list)
    def get_map_entries(self, field_names):
        """
        Internal use for left_join
        """

        if isinstance(field_names, FieldNames):
            assert len(field_names) == 1
            field_names = iter(field_names).next()

        # field_names is now a string
        field_name, _, subfield = field_names.partition(FIELD_SEPARATOR)

        if not subfield:
            if field_name in self._record:
                return [(self._record[field_name], self._record)]
            else:
                return list() 
        else:
            ret = list()
            for record in self._record[field_name]:
                tuple_list = record.get_map_entries(subfield)
                ret.extend(tuple_list)
            return ret

    def _get(self, field_name, default, remove):

        field_name, _, subfield = field_name.partition(FIELD_SEPARATOR)

        if not subfield:
            if remove:
                if default is Unspecified:
                    return dict.pop(self._record, field_name)
                else:
                    return dict.pop(self._record, field_name, default)
            else:
                if default is Unspecified:
                    return dict.get(self._record, field_name)
                else:
                    return dict.get(self._record, field_name, default)
                    
        else:
            if default is Unspecified:
                subrecord = dict.get(self._record, field_name)
            else:
                subrecord = dict.get(self._record, field_name, default)
            if isinstance(subrecord, Records):
                # A list of lists
                return  map(lambda r: r._get(subfield, default, remove), subrecord)
            elif isinstance(subrecord, Record):
                return [subrecord._get(subfield, default, remove)]
            else:
                return [default]

    def get(self, field_name, default = Unspecified):
        return self._get(field_name, default, remove = False)

    def pop(self, field_name, default = Unspecified):
        return self._get(field_name, default, remove = True)

    def set(self, key, value):
        key, _, subkey = key.partition(FIELD_SEPARATOR)

        if subkey:
            if not key in self._record:
                Log.warning("Strange case 1, should not happen often... To test...")
                self._record[key] = Record()
            subrecord = self._record[key]
            if isinstance(subrecord, Records):
                Log.warning("Strange case 2, should not happen often... To test...")
            elif isinstance(subrecord, Record):
                subrecord.set(subkey, value)
            else:
                raise NotImplemented
        else:
            self._record[key] = value

    def get_value(self, field_names):
        """
        Args:
            fields: A String instance (field name), or a set of String instances
                (field names) # XXX tuple !!
        Returns:
            If fields is a String,  return the corresponding value.
            If fields is a FieldNames, return a tuple of corresponding value.

        Raises:
            KeyError if at least one of the fields is not found
        """
        assert isinstance(field_names, (StringTypes, FieldNames)),\
            "Invalid field_names = %s (%s)" % (field_names, type(field_names))

        if isinstance(field_names, StringTypes):
            if '.' in field_names:
                key, _, subkey = key.partition(FIELD_SEPARATOR)
                if not key in self._record:
                    return None
                if isinstance(self._record[key], Records):
                    return [subrecord.get_value(subkey) for subrecord in self._record[key]]
                elif isinstance(self._record[key], Record):
                    return self._record[key].get_value(subkey)
                else:
                    raise Exception, "Unknown field"
            else:
                return self._record[field_names]
        else:
            # XXX see. get_map_entries
            if len(field_names) == 1:
                field_names = iter(field_names).next()
                return self.get_value(field_names)
            return tuple(map(lambda x: self.get_value(x), field_names))


    @returns(bool)
    def has_field_names(self, field_names):
        """
        Test whether a Record carries a set of field names.
        Args:
            field_names: A FieldNames instance.
        Returns:
            True iif record carries this set of field names.
        """
#DEPRECATED|        if isinstance(fields, StringTypes):
#DEPRECATED|            # SHOULD BE DEPRECATED SOON since we are only using the FieldNames()
#DEPRECATED|            # class now...
#DEPRECATED|            return fields in self._record
        assert isinstance(field_names, FieldNames)

        field_names, map_method_subfields, _, _ = field_names.split_subfields()
        if not set(field_names) <= set(self._record.keys()):
            return False

        for method, sub_field_names in map_method_subfields.items():
            # XXX 1..1 not taken into account here
            for record in self._record[method]:
                if not record.has_field_names(sub_field_names):
                    return False
        return True

#DEPRECATED|        # self._record.keys() should have type Fields, otherwise comparison
#DEPRECATED|        # fails without casting to set
#DEPRECATED|        return set(fields) <= set(self._record.keys())

    @returns(bool)
    def has_empty_fields(self, keys):
        """
        Tests whether a Record contains a whole set of field names.
        Args:
            keys: A set of String (corresponding to field names).
        Returns:
            True iif self does not contain all the field names.
        """
        for key in keys:
            if self._record[key]: return False
        return True

#DEPRECATED|    def pop(self, *args, **kwargs):
#DEPRECATED|        return dict.pop(self._record, *args, **kwargs)

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

    @returns(FieldNames)
    def get_field_names(self):
        return FieldNames(self.keys())

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

    def __eq__(self, other):
        return self._record == other._record and self._last == other._last


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

    def get_field_names(self):
        return self.get_one().get_field_names()

    def add_record(self, record):
        self.append(record)

    def add_records(self, records):
        self.extend(records)
