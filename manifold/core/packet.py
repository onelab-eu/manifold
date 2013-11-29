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
from manifold.core.query        import Query
from manifold.util.log          import Log 
from manifold.util.type         import accepts, returns

class Packet(object):
    """
    A generic packet class: Query packet, Record packet, Error packet (ICMP), etc.
    """

    TYPE_QUERY  = 1
    TYPE_RECORD = 2
    TYPE_ERROR  = 3

    #---------------------------------------------------------------------------
    # Helpers for assertions
    #---------------------------------------------------------------------------

    @staticmethod
    @returns(StringTypes)
    def get_type_name(type):
        """
        Returns:
            The String corresponding to the type of Packet.
        """
        TYPE_NAMES = {
            Packet.TYPE_QUERY  : 'QUERY',
            Packet.TYPE_RECORD : 'RECORD',
            Packet.TYPE_ERROR  : 'ERROR'
        }

        return TYPE_NAMES[type]

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, type):
        """
        Constructor.
        Args:
            type: A value among {Packet.TYPE_QUERY, Packet.TYPE_RECORD, Packet.TYPE_ERROR}.
        """
        self._type = type

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(int)
    def get_type(self):
        """
        Returns:
            The type of packet corresponding to this Packet instance.
        """
        return self._type

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
        return "<Packet.%s>" % Packet.get_type_name(self.get_type())

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

    def __init__(self, query, annotation, receiver = None, source = None):
        """
        Constructor
        Args:
            query: A Query instance.
            annotation: An Annotation instance related to the Query or None.
            receiver: The Node instance which will receive this Packet (next hop).
            source:
        """
        Packet.__init__(self, Packet.TYPE_QUERY)
        assert isinstance(query, Query), \
            "Invalid query = %s (%s)" % (query, type(query))
        assert not annotation or isinstance(annotation, Annotation), \
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))

        self._query      = query
        self._annotation = annotation
        self._receiver   = receiver
        self._source     = source

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

    #@returns(QueryPacket)
    def clone(self):
        """
        Returns:
            A Packet instance cloned from self.
        """
        query      = self._query.clone()
        annotation = self._annotation
        receiver   = self._receiver
        return QueryPacket(query, annotation, receiver)

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
    Equivalent to current ResultValue
    Equivalent to current ICMP errors
    """
    def __init__(self, message = None):
        Packet.__init__(self, Packet.TYPE_ERROR)
        self._message = message

    @returns(StringTypes)
    def get_message(self):
        """
        Returns:
            The error message related to this ErrorPacket.
        """
        return self._message
