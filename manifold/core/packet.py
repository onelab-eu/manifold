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

import copy
from types                      import StringTypes

from manifold.core.annotation   import Annotation
from manifold.core.code         import ERROR, WARNING
from manifold.core.exceptions   import ManifoldException
from manifold.core.query        import Query
from manifold.util.log          import Log 
from manifold.util.type         import accepts, returns

class Packet(object):
    """
    A generic packet class: Query packet, Record packet, Error packet (ICMP), etc.
    """

    PROTOCOL_QUERY  = 1
    PROTOCOL_RECORD = 2
    PROTOCOL_ERROR  = 3

    PROTOCOL_NAMES = {
        PROTOCOL_QUERY  : "QUERY",
        PROTOCOL_RECORD : "RECORD",
        PROTOCOL_ERROR  : "ERROR"
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

    def __init__(self, protocol, last = True):
        """
        Constructor.
        Args:
            type: A value among {Packet.PROTOCOL_QUERY, Packet.PROTOCOL_RECORD, Packet.PROTOCOL_ERROR}.
        """
        assert protocol in Packet.PROTOCOL_NAMES.keys(),\
            "Invalid protocol = %s (not in %s)" % (protocol, Packet.PROTOCOL_NAMES.keys())
        assert isinstance(last, bool),\
            "Invalid last = %s (%s)" % (last, type(last))

        self._protocol = protocol
        self._last     = last
        self._source   = None

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
        raise Exception, "Packet::clone() must be overloaded in child classes"

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this QUERY Packet.
        """
        return "<Packet.%s%s>" % (
            Packet.get_protocol_name(self.get_protocol()),
            ' LAST' if self.is_last() else ''
        )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this QUERY Packet.
        """
        return self.__repr__() 

# NOTE: This class will probably disappear and we will use only the Packet class
class QueryPacket(Packet):
    
    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, query, annotation, receiver = None, source = None, records = None):
        """
        Constructor
        Args:
            query: A Query instance.
            annotation: An Annotation instance related to the Query or None.
            receiver: An absolute destination on which RECORD Packet must
                be uploaded 
            source: The issuer of the QueryPacket
        """
        assert isinstance(query, Query), \
            "Invalid query = %s (%s)" % (query, type(query))
        assert not annotation or isinstance(annotation, Annotation), \
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))

        Packet.__init__(self, Packet.PROTOCOL_QUERY)
        #self._destination = query.get_destination()
        self._query       = query
        self._annotation  = annotation
        self._receiver    = receiver
        self._source      = source
        self._records     = records

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(Query)
    def get_query(self):
        """
        Returns:
            The Query nested in this QUERY Packet.
        """
        return self._query
        
    def set_query(self, query):
        """
        Set the Query carried by this QUERY Packet.
        Args:
            query: The new Query installed in this Packet.
        """
        assert isinstance(query, Query)
        self._query = query

    def update_query(self, method, *args, **kwargs):
        self._query = method(self._query, *args, **kwargs)

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            The Annotation nested in this QUERY Packet.
        """
        return self._annotation

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

    def get_source(self):
        return self._source

    def get_destination(self):
        return self._query.get_destination()

    def set_destination(self, destination):
        self._query.set_destination(destination)

    def set_records(self, records):
        self._records = records

    def get_records(self):
        return self._records

    #@returns(QueryPacket)
    def clone(self):
        """
        Returns:
            A Packet instance cloned from self.
        """
        query      = self._query.clone()
        annotation = self._annotation
        receiver   = self._receiver
        records    = copy.deepcopy(self._records)
        return QueryPacket(query, annotation, receiver, records = records)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this QUERY Packet.
        """
        return "%s(%s)" % (self.__repr__(), self.get_query())

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
            The '%r' representation of this QUERY Packet.
        """
        return "<Packet.%s: %s>" % (
            Packet.get_protocol_name(self.get_protocol()),
            self.get_message()
        )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this QUERY Packet.
        """
        return self.__repr__() 
