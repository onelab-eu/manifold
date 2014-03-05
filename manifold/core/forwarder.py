#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Forwarder embeds exactly one Gateway assuming that its
# Announces are already normalized. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.result_value import ResultValue
from manifold.core.interface    import Interface
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
from manifold.core.router       import Router 

class Forwarder(Interface):

    def receive(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A QUERY Packet instance. 
        """
        assert isinstance(packet, Packet),\
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        raise Exception, "SHould not be used"
        # Create a Socket holding the connection information and bind it.
        socket = Socket(consumer = packet.get_receiver())
        packet.set_receiver(socket)

        # Build the AST and retrieve the corresponding root_node Operator instance.
        query = packet.get_query()
        annotation = packet.get_annotation()

        Log.warning("Forwarder::receive not yet implemented")
#DEPRECATED|        # We suppose we have no namespace from here
#DEPRECATED|        qp = QueryPlan()
#DEPRECATED|        qp.build_simple(query, self.metadata, self.allowed_capabilities)
#DEPRECATED|        self.init_from_nodes(qp, user)
#DEPRECATED|
#DEPRECATED|        d = defer.Deferred() if is_deferred else None
#DEPRECATED|        # the deferred object is sent to execute function of the query_plan
#DEPRECATED|        return qp.execute(d, receiver)
