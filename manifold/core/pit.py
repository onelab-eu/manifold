#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# The Pending Interest Table (PIT) of a Manifold Gateway maps
# a Query with a set of Consumers.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from types                          import StringTypes
from pprint                         import pformat

from manifold.core.consumer         import Consumer
from manifold.core.query            import Query
from manifold.core.socket           import Socket
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class Pit(object):
    def __init__(self, producer):
        """
        Constructor.
        Args:
            producer: The Producer that will send RECORD and ERROR
                Packet through this Socket.
        """
        # TODO: Use Lattice instead. 
        self._map_query_socket   = dict() # {Query : set(Consumer)}
        self._map_receiver_query = dict() # {Consumer : Query}
        self._producer           = producer

    @returns(Socket)
    def add_flow(self, query, receiver):
        """
        Add a Consumer for a given Query an a given Consumer and
        return the corresponding Socket.
        A Socket is automatically added in this Pit iif required.
        Args:
            query: A Query instance correponding to a pending Query.
            receiver: A Consumer instance (a From instance most of time). 
        Returns:
            The corresponding Socket.
        """
        assert isinstance(query, Query), \
            "Invalid query = %s (%s)" % (query, type(query))
        assert isinstance(receiver, Consumer), \
            "Invalid receiver = %s (%s)" % (receiver, type(receiver))

        #try:
        #    # If no exception is raised, there is an existing Socket can be reused.
        #    Log.tmp("Why would we reuse a socket here, for a non streaming query")
        #    socket = self._map_query_socket[query]
        #    print "socket from pit", socket
        #    socket.add_producer(receiver) 
        #except KeyError:
        # We need to create a new Socket 
        socket = Socket(receiver)
        # XXX
        socket.set_producer(self._producer, cascade = False) 
        self._map_query_socket[query] = socket 

        self._map_receiver_query[receiver] = query
        return socket

    def del_receiver(self, receiver):
        """
        Delete a flow from this PIT.
        Args:
            receiver: A Consumer instance (a Socket instance most of time). 
        Raises:
            KeyError: if the query and/or the Consumer are not referenced
                in this PIT.
        """
        assert isinstance(receiver, Consumer),\
            "Invalid receiver = %s (%s)" % (receiver, type(receiver))
    
        try:
            query  = self._map_receiver_query[receiver]
            socket = self._map_query_socket[query]
            socket.del_receiver(receiver)
            if socket.is_empty():
                del self._map_query_socket[query]
            del self._map_receiver_query[receiver]
        except KeyError:
            pass

    def del_query(self, query):
        """
        Remove from this PIT a given Query and unreference
        correspoding receivers. 
        Args:
            query: A Query instance.
        Raises:
            KeyError: if query is not in this Pit.
        """
        socket = self._map_query_socket[query]
        for receiver in socket.get_consumers():
            del self._map_receiver_query[receiver]
        del self._map_query_socket[query]

    @returns(Socket)
    def get_socket(self, query):
        """
        Retrieve Socket instance related to a given Query.
        Args:
            query: A Query instance.
        Raises:
            KeyError: if query is not in this Pit.
        Returns:
            The corresponding Socket (if any).
        """
        return self._map_query_socket[query]

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Pit.
        """
        return pformat(self._map_query_socket)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Pit.
        """
        return repr(self) 