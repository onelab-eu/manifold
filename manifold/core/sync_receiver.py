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
from types                      import StringTypes

from manifold.core.consumer     import Consumer
from manifold.core.packet       import Packet
from manifold.core.record       import Records
from manifold.core.result_value import ResultValue
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class SyncReceiver(Consumer):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor.
        """
        Consumer.__init__(self)
        self._records = Records()
        self._event = threading.Event()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def stop(self):
        """
        Stop the SyncReceiver by unlocking its thread.
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
        do_stop = True

        if packet.get_type() == Packet.TYPE_RECORD:
            if not packet.is_last():
                do_stop = False
                self._records.append(packet)
        elif packet.get_type() == Packet.TYPE_ERROR:
            message = packet.get_message()
            trace   = packet.get_traceback()
            raise Exception("%(message)s%(trace)s" % {
                "message" : message if message else "(No message)",
                "trace"   : trace   if trace   else "(No traceback)"
            })
        else:
            Log.warning(
                "SyncReceiver::receive(): Invalid Packet type (%s, %s)" % (
                    packet,
                    Packet.get_type_name(packet.get_type())
                )
            )

        # TODO this flag should be set to True iif we receive a LastRecord
        # Packet (which could be a RECORD or an ERROR Packet). Each Node
        # should manage the "LAST_RECORD" flag while forwarding its Packets.
        if do_stop:
            self.stop()

    @returns(list)
    def get_records(self):
        """
        Returns:
            The list of Record corresponding to a given Query. This function
            is blocking until having fetched the whole set of Records
            corresponding to this Query.
        """
        self._event.wait()
        self._event.clear()
        return self._records.to_list()

    @returns(ResultValue)
    def get_result_value(self):
        """
        Returns:
            The ResultValue corresponding to a given Query. This function
            is blocking until having fetched the whole set of Records
            corresponding to this Query.
        """
        return ResultValue.get_success(self.get_records())
