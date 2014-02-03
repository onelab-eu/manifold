#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldClient is a the base virtual class that
# inherits any Manifold client. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.packet           import Packet, QueryPacket
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log 
from manifold.util.type             import accepts, returns

class ManifoldClient(object):

    def __init__(self):
        """
        Constructor
        """
        self.init_router()

    #--------------------------------------------------------------
    # Child classes may overload/overwrite the following methods.
    #--------------------------------------------------------------

    def init_router(self):
        """
        Method that enforces self.router initialization
        """
        raise NotImplementedError("self.router must be initialized")

    @returns(StringTypes)
    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        Returns:
            A welcome message
        """
        return None

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        return None

    @returns(Annotation)
    def get_annotation():
        """
        (This method is supposed to be overwritten in child classes).
        Returns:
            An additionnal Annotation to pass to the QUERY Packet
            sent to the Router.
        """
        return Annotation() 

    #--------------------------------------------------------------
    # Common methods
    #--------------------------------------------------------------

    def send(self, packet):
        """
        Send a Packet to the nested Manifold Router.
        Args:
            packet: A QUERY Packet instance.
        """
        assert isinstance(packet, Packet), \
            "Invalid packet %s (%s)" % (packet, type(packet))
        assert packet.get_protocol() == Packet.PROTOCOL_QUERY, \
            "Invalid packet %s of type %s" % (
                packet,
                Packet.get_protocol_name(packet.get_protocol())
            )
        self.router.receive(packet)

    @returns(ResultValue)
    def forward(self, query, annotation = None):
        """
        Send a Query to the nested Manifold Router.
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """
        if not annotation:
            annotation = Annotation()
        annotation |= self.get_annotation() 

        receiver = SyncReceiver()
        packet = QueryPacket(query, annotation, receiver = receiver)
        self.send(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value


