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

import copy, pickle, uuid
from types                          import GeneratorType, StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.code             import ERROR, WARNING
from manifold.core.exceptions       import ManifoldException
from manifold.core.field_names      import FieldNames, FIELD_SEPARATOR
from manifold.core.flow             import Flow
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class Unspecified(object):
    pass

class Packet(object):
    """
    A generic packet class: Query packet, Record packet, Error packet (ICMP), etc.
    """

    PROTOCOL_NONE   = 0
    PROTOCOL_QUERY  = 1
#    PROTOCOL_RECORD = 2
    PROTOCOL_ERROR  = 3
#LOIC|    PROTOCOL_GET    = 4
#LOIC|    PROTOCOL_CREATE = 5
#LOIC|    PROTOCOL_UPDATE = 6
#LOIC|    PROTOCOL_DELETE = 7
    PROTOCOL_PING   = 8

#LOIC|    PROTOCOL_QUERY_TYPES = [PROTOCOL_GET, PROTOCOL_UPDATE, PROTOCOL_DELETE] # TEMP, Records == CREATE

    PROTOCOL_NAMES = {
        PROTOCOL_QUERY  : "QUERY",
#        PROTOCOL_RECORD : "RECORD",
        PROTOCOL_ERROR  : "ERROR",
#LOIC|        PROTOCOL_GET    : "GET",
#LOIC|        PROTOCOL_CREATE : "CREATE",
#LOIC|        PROTOCOL_UPDATE : "UPDATE",
#LOIC|        PROTOCOL_DELETE : "DELETE",
        PROTOCOL_PING   : "PING",
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
        return Packet.PROTOCOL_NAMES.get(type, 'UNKNOWN')

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(
        self,
        receiver = None,
        last = True,
        protocol = PROTOCOL_NONE,
        src_addr = None,
        dst_addr = None
    ):
        """
        Constructor.
        Args:
            receiver:
            last: A boolean indicating whether this Packet is the last related to a Flow.
            protocol: A value among PROTOCOL_*
            src_addr: The source Address.
            dst_addr: The destination Address.
        """
        assert isinstance(last, bool),\
            "Invalid last = %s (%s)" % (last, type(last))

        self._receiver      = receiver
        self._last          = last
        self._protocol      = protocol
        self._source        = src_addr
        self._destination   = dst_addr

        self._annotation    = Annotation()
        self._ttl           = 0

        # If this is a PROTOCOL_QUERY Packet, self._data contains Query.action & Query.args
        self._data          = None

        # We internally attach some data to packets
        self._ingress       = None
        self._uuid          = str(uuid.uuid4())

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

    def set_protocol(self, protocol):
        self._protocol = protocol

    @returns(bool)
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

    def update_source(self, method, *args, **kwargs):
        self.set_source(method(self.get_source(), *args, **kwargs))

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

    @returns(int)
    def get_ttl(self):
        """
        Returns:
            Return the Time-To-Live of this Packet.
        """
        return self._ttl

    def set_ttl(self, ttl):
        """
        Set the Time-To-Live of this Packet.
        Args:
            ttl: An integer.
        """
        self._ttl = ttl

    def inc_ttl(self):
        """
        Increment the Time-To-Live of this Packet.
        """
        self._ttl += 1

    # Compatibility
    def set_query(self, query):
        Log.warning("DEPRECATED: set_query(): use set_destination() and set_data()")
        self.set_destination(query.get_destination())
        self.set_data(query.get_data())

# Use packet_update_query(packet,  method, *args, **kwargs)
#
#DEPRECATED|    def update_query(self, method, *args, **kwargs):
#DEPRECATED|        from manifold.core.query import Query
#DEPRECATED|        Log.warning("DEPRECATED: update_query(): use set_destination() and set_data()")
#DEPRECATED|        self.set_query(method(QueryFactory.from_packet(self), *args, **kwargs))

    def get_data(self):
        return self._data

    def set_data(self, data):
        self._data = data

    def update_data(self, data):
        """
        Args:
            data: either a Packet instance or a dict.
        """
        Log.tmp(data)
        assert isinstance(data, (Packet, dict))
        if isinstance(data, Packet):
            assert self.get_protocol() == PROTOCOL_QUERY
        if not self._data:
            self._data = {}
        self._data.update(data)

    def clear_data(self):
        self._data = None

    def get_flow(self):
        return Flow(self._source, self._destination)

    def get_uuid(self):
        return self._uuid

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(dict)
    def get_dict(self):
        """
        Returns:
            The dict nested in this Packet. Note that most of time
            you should use to_dict() method instead.
        """
        return self._data

    def set_dict(self, dic):
         self._data = dic

    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dict representation of this Packet.
        """
        dic = dict()
        if self._data:
            try:
                for k, v in self._data.iteritems():
                    if isinstance(v, Packet):
                        dic[k] = v.to_dict()
                    elif isinstance(v, list) and not type(v) == list:
                        dic[k] = v.to_list()
                    else:
                        dic[k] = v
            except Exception, e:
                print "EEEEEE", e
        return dic

    @staticmethod
    def from_dict(dic):
        packet = Packet()
        packet.set_dict(dic)
        return packet

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif this Packet is the last one of a list
            of Packets corresponding to a given Query.
        """
        return self._data is None

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    def __getitem__(self, key):
        """
        Extract from this Packet a field value.
        Args:
            key: A String instance corresponding to a field name
                of this Packet.
        Returns:
            The corresponding value.
        """
        if not self._data:
            raise Exception, "Empty record"
        return self.get(key)
#        return dict.__getitem__(, key, **kwargs)

    def __setitem__(self, key, value, **kwargs):
        """
        Set the value corresponding to a given key.
        Args:
            key: A String instance corresponding to a field name
                of this Packet.
            value: The value that must be mapped with this key.
        """
        if not self._data:
            self._data = dict()
        return dict.__setitem__(self._data, key, value, **kwargs)

    def __iter__(self):
        """
        Returns:
            A dictionary-keyiterator allowing to iterate on fields
            of this Packet.
        """
        if self._data is None:
            return dict.__iter__({})
        return dict.__iter__(self._data)

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
            if field_name in self._data:
                return [(self._data[field_name], self._data)]
            else:
                return list()
        else:
            ret = list()
            for record in self._data[field_name]:
                tuple_list = record.get_map_entries(subfield)
                ret.extend(tuple_list)
            return ret

    def _get(self, field_name, default, remove):

        field_name, _, subfield = field_name.partition(FIELD_SEPARATOR)

        if not subfield:
            if remove:
                if default is Unspecified:
                    return dict.pop(self._data, field_name)
                else:
                    return dict.pop(self._data, field_name, default)
            else:
                if default is Unspecified:
                    return dict.get(self._data, field_name)
                else:
                    return dict.get(self._data, field_name, default)

        else:
            if default is Unspecified:
                subrecord = dict.get(self._data, field_name)
            else:
                subrecord = dict.get(self._data, field_name, default)
            if isinstance(subrecord, list):
                # A list of lists
                return  map(lambda r: r._get(subfield, default, remove), subrecord)
            elif isinstance(subrecord, Packet):
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
            if not key in self._data:
                Log.warning("Strange case 1, should not happen often... To test...")
                self._data[key] = Packet()
            subrecord = self._data[key]
            if isinstance(subrecord, list):
                Log.warning("Strange case 2, should not happen often... To test...")
            elif isinstance(subrecord, Packet):
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
                if not key in self._data:
                    return None
                if isinstance(self._data[key], list):
                    return [subrecord.get_value(subkey) for subrecord in self._data[key]]
                elif isinstance(self._data[key], Packet):
                    return self._data[key].get_value(subkey)
                else:
                    raise Exception, "Unknown field"
            else:
                return self._data[field_names]
        else:
            # XXX see. get_map_entries
            if len(field_names) == 1:
                field_names = iter(field_names).next()
                return self.get_value(field_names)
            return tuple(map(lambda x: self.get_value(x), field_names))

    @returns(bool)
    def has_field_names(self, field_names):
        """
        Test whether a Packet carries a set of field names.
        Args:
            field_names: A FieldNames instance.
        Returns:
            True iif record carries this set of field names.
        """
#DEPRECATED|        if isinstance(fields, StringTypes):
#DEPRECATED|            # SHOULD BE DEPRECATED SOON since we are only using the FieldNames()
#DEPRECATED|            # class now...
#DEPRECATED|            return fields in self._data
        assert isinstance(field_names, FieldNames)

        field_names, map_method_subfields, _, _ = field_names.split_subfields()
        if not set(field_names) <= set(self._data.keys()):
            return False

        for method, sub_field_names in map_method_subfields.items():
            # XXX 1..1 not taken into account here
            for record in self._data[method]:
                if not record.has_field_names(sub_field_names):
                    return False
        return True

#DEPRECATED|        # self._data.keys() should have type Fields, otherwise comparison
#DEPRECATED|        # fails without casting to set
#DEPRECATED|        return set(fields) <= set(self._data.keys())

    @returns(bool)
    def has_empty_fields(self, keys):
        """
        Tests whether a Packet contains a whole set of field names.
        Args:
            keys: A set of String (corresponding to field names).
        Returns:
            True iif self does not contain all the field names.
        """
        for key in keys:
            if self._data[key]: return False
        return True

#DEPRECATED|    def pop(self, *args, **kwargs):
#DEPRECATED|        return dict.pop(self._data, *args, **kwargs)

    def items(self):
        return dict.items(self._data) if self._data else list()

    @returns(list)
    def keys(self):
        """
        Returns:
            A list of String where each String correspond to a field
            name of this Packet.
        """
        return dict.keys(self._data) if self._data else list()

    @returns(FieldNames)
    def get_field_names(self):
        return FieldNames(self.keys())

    def update(self, other_record):
        return dict.update(self._data, other_record)

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
        clone._uuid = str(uuid.uuid4())
        clone.set_receiver(self.get_receiver())
        return clone
    copy = clone

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Packet.
        """
        fmt = "\n\t".join([
            "<Packet",
            "type       : %(type)s",
            "uuid       : %(uuid)s",
            "annotation : %(annotation)s",
            "last       : %(last)s",
            "src        : %(src)s",
            "dst        : %(dst)s",
            "data       : %(data)s\n>"
        ])
        return fmt % {
            "type"       : Packet.get_protocol_name(self.get_protocol()),
            "uuid"       : "%s" % self._uuid,
            "annotation" : "%r" % self.get_annotation() if self.get_annotation else '',
            "last"       : self.is_last(),
            "src"        : self.get_source(),
            "dst"        : self.get_destination(),
            "data"       : ' '.join([("%s" % self._data) if self._data else ''])
        }


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
            receiver: A FIB or From or SyncReceiver instance.
        """
        self._receiver = receiver

    @classmethod
    @returns(dict)
    def from_key_value(self, key, value):
        if isinstance(key, StringTypes):
            return {key : value}
        else:
            return Packet(izip(key, value))

# See PacketFactory
#LOIC|class PING(Packet):
#LOIC|    def __init__(self):
#LOIC|        Packet.__init__(self, **kwargs)
#LOIC|        self.set_protocol(Packet.PROTOCOL_PING)
#LOIC|
#LOIC|class GET(Packet):
#LOIC|    def __init__(self, **kwargs):
#LOIC|        Packet.__init__(self, **kwargs)
#LOIC|        self.set_protocol(Packet.PROTOCOL_GET)
#LOIC|
#LOIC|class CREATE(Packet):
#LOIC|    def __init__(self, **kwargs):
#LOIC|        Packet.__init__(self, **kwargs)
#LOIC|        self.set_protocol(Packet.PROTOCOL_CREATE)
#LOIC|
#LOIC|class UPDATE(Packet):
#LOIC|    def __init__(self):
#LOIC|        Packet.__init__(self, **kwargs)
#LOIC|        self.set_protocol(Packet.PROTOCOL_UPDATE)
#LOIC|
#LOIC|class DELETE(Packet):
#LOIC|    def __init__(self, **kwargs):
#LOIC|        Packet.__init__(self, **kwargs)
#LOIC|        self.set_protocol(Packet.PROTOCOL_DELETE)
#LOIC|

# Error types
E_TYPE_TIMEOUT = 28
E_CODE_TIMEOUT = 28
E_MSG_TIMEOUT_TIMEOUT = "Timeout on pending flow entry"

# NOTE: This class will probably disappear and we will use only the Packet class
class ErrorPacket(Packet):
    """
    Analog with ICMP errors packets in IP networks
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, type = ERROR, code = ERROR, message = None, traceback = None, **kwargs):
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

        Packet.__init__(self, **kwargs)
        self.set_protocol(Packet.PROTOCOL_ERROR)
        self.set_last()
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

class TimeoutErrorPacket(ErrorPacket):
    def __init__(self, **kwargs):
        if not 'message' in kwargs:
            kwargs['message'] = E_MSG_TIMEOUT_TIMEOUT
        if not 'last' in kwargs:
            kwargs['last'] = True
        ErrorPacket.__init__(self, type=E_TYPE_TIMEOUT, code=E_CODE_TIMEOUT, **kwargs)

