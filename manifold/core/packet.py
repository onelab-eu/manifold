# -*- coding: utf-8 -*-

from copy                 import deepcopy

from manifold.core.record import Record

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

    TYPE_NAMES = {
        TYPE_QUERY  : 'QUERY',
        TYPE_RECORD : 'RECORD',
        TYPE_ERROR  : 'ERROR'
    }

    @staticmethod
    def get_type_name(self, type):
        return self.TYPE_NAMES[type]

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

    # UNUSED ?
    def clone():
        return copy.deepcopy(packet)

class QueryPacket(Packet):
    
    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, query, annotations, receiver = None, source = None):
        Packet.__init__(self, TYPE_QUERY)

        self._query       = query
        self._annotations = annotations
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

    def get_annotations(self):
        return self._annotations

    def get_receiver(self):
        return self._destination

    def get_source(self):
        return self._destination

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

class RecordPacket(Packet, Record):
    """
    Equivalent to current Record
    """
    def __init__(self):
        Packet.__init__(self, TYPE_RECORD)

class ErrorPacket(Packet):
    """
    Equivalent to current ResultValue
    Equivalent to current ICMP errors
    """
    def __init__(self):
        Packet.__init__(self, TYPE_ERROR)
