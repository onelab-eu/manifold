# -*- coding: utf-8 -*-

import copy

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
    def get_type_name(type):

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
        self._type = type

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_type(self):
        return self._type

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def clone(self):
        raise Exception, "To be implemented in child classes"

class QueryPacket(Packet):
    
    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, query, annotation, receiver = None, source = None):
        Packet.__init__(self, Packet.TYPE_QUERY)

        self._query       = query
        self._annotation = annotation
        self._receiver    = receiver
        self._source      = source

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_query(self):
        return self._query
        
    def set_query(self, query):
        self._query = query

    def update_query(self, method, *args, **kwargs):
        self._query = method(self._query, *args, **kwargs)

    def get_annotation(self):
        return self._annotation

    def get_receiver(self):
        return self._receiver

    def set_receiver(self, receiver):
        self._receiver = receiver

    def get_source(self):
        return self._source

    def clone(self):
        query      = self._query.clone()
        annotation = self._annotation
        receiver   = self._receiver
        return QueryPacket(query, annotation, receiver)


class ErrorPacket(Packet):
    """
    Equivalent to current ResultValue
    Equivalent to current ICMP errors
    """
    def __init__(self):
        Packet.__init__(self, Packet.TYPE_ERROR)