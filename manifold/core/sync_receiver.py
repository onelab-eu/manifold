#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# SyncReceiver class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import threading
from types                       import StringTypes

from manifold.core.operator_slot import ChildSlotMixin
from manifold.core.node          import Node
from manifold.core.packet        import Packet
from manifold.core.record        import Records
from manifold.core.result_value  import ResultValue
from manifold.util.log           import Log
from manifold.util.type          import accepts, returns

class SyncReceiver(Node, ChildSlotMixin):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor. Lifetime of a SyncReceiver corresponds to the Query
        it transports and the corresponding results retrieval.
        """
        Node.__init__(self)
        ChildSlotMixin.__init__(self)
        self._records = Records() # Records resulting from a Query
        self._errors = list()     # ResultValue to errors which have occured 
        self._event = threading.Event()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def terminate(self):
        """
        Stop this SyncReceiver.
        """
        self._event.set()

    def receive(self, packet):
        """
        Process an incoming Packet received by this SyncReceiver instance.
        Args:
            packet: A Packet instance. If this is a RECORD Packet, its
                corresponding record is bufferized in this SyncReceiver
                until records retrieval.
        """
        if packet.get_protocol() == Packet.PROTOCOL_RECORD:
            if not packet.is_empty():
                self._records.append(packet)
        elif packet.get_protocol() == Packet.PROTOCOL_ERROR:
            self._errors.append(packet) # .get_exception()
        else:
            Log.warning(
                "SyncReceiver::receive(): Invalid Packet type (%s, %s)" % (
                    packet,
                    Packet.get_protocol_name(packet.get_protocol())
                )
            )

        # TODO this flag should be set to True iif we receive a LastRecord
        # Packet (which could be a RECORD or an ERROR Packet). Each Node
        # should manage the "LAST_RECORD" flag while forwarding its Packets.
        if packet.is_last():
            self.terminate()

    @returns(ResultValue)
    def get_result_value(self):
        """
        Returns:
            The ResultValue corresponding to a given Query. This function
            is blocking until having fetched the whole set of Records
            corresponding to this Query.
        """
        self._event.wait()
        self._event.clear()
        return ResultValue.get(self._records, self._errors)