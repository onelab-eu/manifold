#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# DeferredReceiver class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                      import StringTypes
from twisted.internet.defer     import Deferred

# from manifold.core.code        import FORBIDDEN
from manifold.core.node         import Node
from manifold.core.operator_slot import ChildSlotMixin
from manifold.core.packet       import Packet, Records
from manifold.core.result_value import ResultValue
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

# XXX Do we need receivers to inherit from Consumer ?
class DeferredReceiver(Node, ChildSlotMixin):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor.
        """
        Node.__init__(self)
        ChildSlotMixin.__init__(self)

        self._records  = Records()
        self._errors   = Records()
        self._deferred = Deferred()

        # BUGFIX
        
    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

#DEPRECATED|        def process_results(rv):
#DEPRECATED|            if 'description' in rv and isinstance(rv['description'], list):
#DEPRECATED|                rv['description'] = [dict(x) for x in rv['description']]
#DEPRECATED|            # Print Results
#DEPRECATED|            return dict(rv)
#DEPRECATED|
#DEPRECATED|        def handle_exceptions(failure):
#DEPRECATED|            e = failure.trap(Exception)
#DEPRECATED|
#DEPRECATED|            Log.warning("XMLRPCAPI::xmlrpc_forward: Authentication failed: %s" % failure)
#DEPRECATED|
#DEPRECATED|            msg ="XMLRPC error : %s" % e
#DEPRECATED|            return dict(ResultValue.error(msg, FORBIDDEN))
#DEPRECATED|
#DEPRECATED|        # deferred receives results asynchronously
#DEPRECATED|        # Callbacks are triggered process_results if success and handle_exceptions if errors
#DEPRECATED|        deferred.addCallbacks(process_results, handle_exceptions)
#DEPRECATED|        return deferred

    def receive(self, packet, slot_id = None):
        """
        Process an incoming Packet received by this SyncReceiver instance.
        Args:
            packet: A Packet instance. If this is a RECORD Packet, its
                corresponding record is bufferized in this SyncReceiver
                until records retrieval.
        """

        # XXX We should accumulate records and errors here to build up the ResultValue
        if packet.get_protocol() == Packet.PROTOCOL_CREATE:
            if not packet.is_empty():
                self._records.append(packet)
        elif packet.get_protocol() == Packet.PROTOCOL_ERROR:
            self._errors.append(packet)
            #message = packet.get_message()
            #trace   = packet.get_traceback()
            #raise Exception("%(message)s%(trace)s" % {
            #    "message" : message if message else "(No message)",
            #    "trace"   : trace   if trace   else "(No traceback)"
            #})
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
            result_value = ResultValue.get(self._records, self._errors)
            self._deferred.callback(result_value)

    #@returns(Deferred)
    def get_deferred(self):
        """
        Returns:
            The Deferred instance nested in this DeferredReceiver.
        """
        return self._deferred
