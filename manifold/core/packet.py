#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Packets are exchanged between Manifold Nodes (consumer or producer). See:
#    manifold/core/node.py
#    manifold/core/producer.py
#    manifold/core/consumer.py
#    manifold/core/relay.py
#
# Manifold operates hop by hop (like MAC) and a packet may transport:
#   - the identifier of the Node which has just sent it (named source)
#   - the identifier of the next Node (named receiver)
#
# There are 3 kinds of Packets in Manifold (depending on the nature
# of its source and its receiver):
#
#    Type   | Role                             | See also
#    -------+----------------------------------+------------------------
#    QUERY  | carries a Query instance         | manifold/core/query.py
#    RECORD | carries a Record instance        | manifold/core/record.py
#    ERROR  | are created if an error occurs.  |
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import copy, pickle
from types                      import GeneratorType, StringTypes

from manifold.core.annotation   import Annotation
from manifold.core.code         import ERROR, WARNING
from manifold.core.destination  import Destination
from manifold.core.exceptions   import ManifoldException
from manifold.core.field_names  import FieldNames, FIELD_SEPARATOR
from manifold.core.query        import Query
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class Unspecified(object):
    pass

class Flow(object):
    def __init__(self, source, destination):
        self._source = source
        self._destination = destination

    def get_source(self):
        return self._source

    def get_destination(self):
        return self._destination

    def __eq__(self, other):
        is_direct  = self._source == other._source and \
                     self._destination == other._destination
        is_reverse = self._source == other._destination and \
                     self._destination == other._source
        return is_direct or is_reverse
        #return self._record == other._record and self._last == other._last

    def __hash__(self):
        return hash(frozenset([self._source, self._destination]))

    def __repr__(self):
        return "<Flow %s -> %s>" % (self.get_source(), self.get_destination())

class Packet(object):
    """
    A generic packet class: Query packet, Record packet, Error packet (ICMP), etc.
    """

#    PROTOCOL_QUERY  = 1
#    PROTOCOL_RECORD = 2
    PROTOCOL_ERROR  = 3
    PROTOCOL_GET    = 4
    PROTOCOL_CREATE = 5
    PROTOCOL_UPDATE = 6
    PROTOCOL_DELETE = 7
    PROTOCOL_PING = 8

    PROTOCOL_QUERY = (PROTOCOL_GET, PROTOCOL_UPDATE, PROTOCOL_DELETE) # TEMP, Records == CREATE

    PROTOCOL_NAMES = {
        PROTOCOL_QUERY  : "QUERY",
#        PROTOCOL_RECORD : "RECORD",
        PROTOCOL_ERROR  : "ERROR",
        PROTOCOL_GET    : "GET",
        PROTOCOL_CREATE : "CREATE",
        PROTOCOL_UPDATE : "UPDATE",
        PROTOCOL_DELETE : "DELETE",
        PROTOCOL_PING : "PING",
    }

    #---------------------------------------------------------------------------
    # Helpers for assertions
    #---------------------------------------------------------------------------

    @staticmethod
    @returns(StringTypes)
    def get_protocol_name(type):
        """
        Returns:
            The String corresponding to the type of Packet.
        """
        return Packet.PROTOCOL_NAMES[type]

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, protocol, receiver = None, last = True):
        """
        Constructor.
        Args:
            type: A value among {Packet.PROTOCOL_*}.
        """
        assert protocol in Packet.PROTOCOL_NAMES.keys(),\
            "Invalid protocol = %s (not in %s)" % (protocol, Packet.PROTOCOL_NAMES.keys())
        assert isinstance(last, bool),\
            "Invalid last = %s (%s)" % (last, type(last))

        self._source        = None
        self._destination   = None
        self._receiver      = receiver

        self._protocol      = protocol
        self._annotation    = Annotation()

        # Flags
        self._last          = last

        self._record        = None

        # We internally attach some data to packets
        self._ingress       = None

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(int)
    def get_protocol(self):
        """
        Returns:
            The type of packet corresponding to this Packet instance.
        """
        return self._protocol

    def is_last(self):
        return self._last

    def set_last(self, value = True):
        self._last = value

    def unset_last(self):
        self._last = False

    def set_source(self, source):
        self._source = source

    def get_source(self):
        return self._source

    def set_destination(self, destination):
        self._destination = destination

    def get_destination(self):
        return self._destination

    def update_destination(self, method, *args, **kwargs):
        self.set_destination(method(self.get_destination(), *args, **kwargs))

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            The Annotation nested in this QUERY Packet.
        """
        return self._annotation

    def update_annotation(self, annotation):
        self._annotation.update(annotation)

    # Compatibility
    def get_query(self):
        Log.warning("Distinction between params.keys() and fields is not so good")
        destination = self.get_destination()

        object_name = destination.get_object()
        namespace   = destination.get_namespace()
        filters     = destination.get_filter()
        field_names = destination.get_field_names()

        params      = self.get_data()

        if namespace:
            query = Query.get("%s:%s" % (namespace, object_name))
        else:
            query = Query.get(object_name)
        if filters:
            query.filter_by(filters)
        if field_names:
            query.select(field_names)
        if params:
            query.set(params)

        return query

    def set_query(self, query):
        self.set_destination(query.get_destination())
        self.set_data(query.get_data())

    def update_query(self, method, *args, **kwargs):
        Log.warning("update_query is to be deprecated")
        self.set_query(method(self.get_query(), *args, **kwargs))

    def get_data(self):
        return self._record

    def set_data(self, data):
        self._record = data

    def get_flow(self):
        return Flow(self._source, self._destination)

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

    @staticmethod
    def from_dict(dic):
        record = Record()
        record.set_dict(dic)
        return record

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
#        return dict.__getitem__(, key, **kwargs)

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
#        return .get(value, default)

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
            [key] = value


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

#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __repr__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The '%r' representation of this QUERY Packet.
#DEPRECATED|        """
#DEPRECATED|        return "<Packet.%s %s>" % (
#DEPRECATED|            Packet.get_protocol_name(self.get_protocol()),
#DEPRECATED|            self.to_dict()
#DEPRECATED|        )
#DEPRECATED|
#DEPRECATED|    def get_uuid(self):
#DEPRECATED|        if not self._uuid:
#DEPRECATED|            self._uuid = str(uuid.uuid4())
#DEPRECATED|        return self._uuid
#DEPRECATED|
#DEPRECATED|    def set_parent_uuid(self, uuid):
#DEPRECATED|        self._parent_uuid = uuid

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    #@returns(Packet)
    def clone(self):
        """
        This method MUST BE overwritten on the class inheriting Packet.
        Returns:
            A Packet instance cloned from self.
        """
        clone = copy.deepcopy(self)
        clone.set_receiver(self.get_receiver())
        return clone
    copy = clone

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Packet.
        """
        return "<Packet.%s%s %s -> %s [DATA: %s]>" % (
            Packet.get_protocol_name(self.get_protocol()),
            ' LAST' if self.is_last() else '',
            self.get_source(), self.get_destination(),
            ' '.join([("%s" % self._record) if self._record else '']),
        )


    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this QUERY Packet.
        """
        return self.__repr__()

    #---------------------------------------------------------------------------
    # Serialization / deserialization
    #---------------------------------------------------------------------------

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
#DEPRECATED|        if '_source' in state:
#DEPRECATED|            del state['_source']
        if '_receiver' in state:
            del state['_receiver']
        return state

    def serialize(self):
        string = pickle.dumps(self)
        return string

    @staticmethod
    def deserialize(string):
        packet = pickle.loads(string)
        packet._receiver = None
        return packet

    #@returns(Node)
    def get_receiver(self):
        """
        Returns:
            The Node which will receive this Packet (next hop).
        """
        return self._receiver

    def set_receiver(self, receiver):
        """
        Set the next hop which will receive this Packet.
        Args:
            receiver: A Node instance.
        """
        self._receiver = receiver


#DEPRECATED|# NOTE: This class will probably disappear and we will use only the Packet class
#DEPRECATED|class QueryPacket(Packet):
#DEPRECATED|
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|    # Constructor
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|    def __init__(self, query, annotation, receiver = None, source = None, records = None):
#DEPRECATED|        """
#DEPRECATED|        Constructor
#DEPRECATED|        Args:
#DEPRECATED|            query: A Query instance.
#DEPRECATED|            annotation: An Annotation instance related to the Query or None.
#DEPRECATED|            receiver: An absolute destination on which RECORD Packet must
#DEPRECATED|                be uploaded
#DEPRECATED|            source: The issuer of the QueryPacket
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(query, Query), \
#DEPRECATED|            "Invalid query = %s (%s)" % (query, type(query))
#DEPRECATED|        assert not annotation or isinstance(annotation, Annotation), \
#DEPRECATED|            "Invalid annotation = %s (%s)" % (annotation, type(annotation))
#DEPRECATED|
#DEPRECATED|        Log.warning("QUERY PACKETS ARE DEPRECATED")
#DEPRECATED|
#DEPRECATED|        Packet.__init__(self, Packet.PROTOCOL_QUERY, receiver)
#DEPRECATED|        #self._destination = query.get_destination()
#DEPRECATED|        self._query       = query
#DEPRECATED|        self._annotation  = annotation
#DEPRECATED|        self._source      = source
#DEPRECATED|        s     = records
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __repr__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The '%r' representation of this ERROR Packet.
#DEPRECATED|        """
#DEPRECATED|        return "<Packet.%s: %s>" % (
#DEPRECATED|            Packet.get_protocol_name(self.get_protocol()),
#DEPRECATED|            self._query
#DEPRECATED|        )
#DEPRECATED|
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|    # Accessors
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|    @returns(Query)
#DEPRECATED|    def get_query(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The Query nested in this QUERY Packet.
#DEPRECATED|        """
#DEPRECATED|        return self._query
#DEPRECATED|
#DEPRECATED|    def set_query(self, query):
#DEPRECATED|        """
#DEPRECATED|        Set the Query carried by this QUERY Packet.
#DEPRECATED|        Args:
#DEPRECATED|            query: The new Query installed in this Packet.
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(query, Query)
#DEPRECATED|        self._query = query
#DEPRECATED|
#DEPRECATED|    def update_query(self, method, *args, **kwargs):
#DEPRECATED|        self._query = method(self._query, *args, **kwargs)
#DEPRECATED|
#DEPRECATED|
#DEPRECATED|    def get_source(self):
#DEPRECATED|        return self._source
#DEPRECATED|
#DEPRECATED|    def get_destination(self):
#DEPRECATED|        return self._query.get_destination()
#DEPRECATED|
#DEPRECATED|    def set_destination(self, destination):
#DEPRECATED|        self._query.set_destination(destination)
#DEPRECATED|
#DEPRECATED|    # XXX In records, we are storing the parent record uuids. This is currently
#DEPRECATED|    # used for the local cache until a better solution is found
#DEPRECATED|    def set_records(self, records):
#DEPRECATED|        s = records
#DEPRECATED|    def get_records(self):
#DEPRECATED|        return s
#DEPRECATED|
#DEPRECATED|    #@returns(QueryPacket)
#DEPRECATED|    def clone(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            A Packet instance cloned from self.
#DEPRECATED|        """
#DEPRECATED|        query      = self._query.clone()
#DEPRECATED|        annotation = self._annotation
#DEPRECATED|        receiver   = self._receiver
#DEPRECATED|        try:
#DEPRECATED|            records    = copy.deepcopy(s)
#DEPRECATED|        except Exception, e:
#DEPRECATED|            print "exception in clone", e
#DEPRECATED|            import pdb; pdb.set_trace()
#DEPRECATED|        return QueryPacket(query, annotation, receiver, records = records)
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __str__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The '%s' representation of this QUERY Packet.
#DEPRECATED|        """
#DEPRECATED|        return "%s(%s)" % (self.__repr__(), self.get_query())

class Record(Packet):
    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """

        protocol = kwargs.pop('protocol', Packet.PROTOCOL_CREATE)

        packet_kwargs = dict()
        packet_kwargs['last'] = kwargs.pop('last', False)
        packet_kwargs['receiver'] = kwargs.pop('receiver', None)
        Packet.__init__(self, protocol = protocol, **packet_kwargs)

        if args or kwargs:
            self._record = dict()
            if len(args) == 1:
                self._record.update(args[0])
            elif len(args) > 1:
                raise Exception, "Bad initializer for Record: %r" % (args,)

            self._record.update(kwargs)
            
        else:
            # We need None to test whether the record is empty
             self._record = None

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

class PING(Packet):
    def __init__(self):
        Packet.__init__(self, Packet.PROTOCOL_PING)

class GET(Packet):
    def __init__(self):
        Packet.__init__(self, Packet.PROTOCOL_GET)

class CREATE(Record):
    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        Record.__init__(self, *args, protocol = Packet.PROTOCOL_CREATE, **kwargs)

class UPDATE(Record):
    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        Record.__init__(self, *args, protocol = Packet.PROTOCOL_UPDATE, **kwargs)

class DELETE(Packet):
    def __init__(self):
        Packet.__init__(self, Packet.PROTOCOL_DELETE)


# NOTE: This class will probably disappear and we will use only the Packet class
class ErrorPacket(Packet):
    """
    Analog with ICMP errors packets in IP networks
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, type = ERROR, code = ERROR, message = None, traceback = None):
        """
        Constructor.
        Args:
            type: An integer among {code::ERROR, code::WARNING}
            code: An integer corresponding to a code defined in manifold.core.code
            message: A String containing the error message or None.
            traceback: A String containing the traceback or None.
        """
        # XXX Wrong !! type for error packets has nothing to do with ERROR WARNING
        # assert type in [WARNING, ERROR]
        assert isinstance(code, int)
        assert not message   or isinstance(message, StringTypes)
        assert not traceback or isinstance(traceback, StringTypes)

        Packet.__init__(self, Packet.PROTOCOL_ERROR)
        self._type      = type
        self._code      = code
        self._message   = message
        self._traceback = traceback

    #---------------------------------------------------------------------------
    # Static methods
    #---------------------------------------------------------------------------

    # XXX This function could take kwargs parameters to set last to False for example
    @staticmethod
    def from_exception(e):
        if not isinstance(e, ManifoldException):
            e = ManifoldException(e)
            import traceback
            traceback.print_exc()
        ret = ErrorPacket(e.TYPE, e.CODE, str(e))
        return ret

    @returns(StringTypes)
    def get_message(self):
        """
        Returns:
            The error message related to this ErrorPacket.
        """
        return self._message

    @returns(StringTypes)
    def get_traceback(self):
        """
        Returns:
            The traceback related to this ErrorPacket.
        """
        return self._traceback

    @returns(int)
    def get_origin(self):
        """
        Returns:
            A value among {code::CORE, code::GATEWAY}
            identifying who is the origin of this ErrorPacket.
        """
        return self._origin

    @returns(int)
    def get_code(self):
        """
        Returns:
            The error code of the Error carried by this ErrorPacket.
            See manifold.core.result_value
        """
        return self._code

    @returns(int)
    def get_type(self):
        """
        Returns:
            The error type of the Error carried by this ErrorPacket.
            See manifold.core.result_value
        """
        return self._type

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this ERROR Packet.
        """
        return "<Packet.%s: %s>" % (
            Packet.get_protocol_name(self.get_protocol()),
            self.get_message()
        )
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
