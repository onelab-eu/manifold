#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Relay Node is simultaneously a Producer and Receiver.
#
# Examples:
#  - Manifold Gateways  (manifold/gateways)
#  - Manifold Operators (manifold/operators)
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>


from manifold.core.packet   import Packet
from manifold.core.producer import Producer
from manifold.core.consumer import Consumer

class Relay(Producer, Consumer):
    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = None):
        Producer.__init__(self, consumers = consumers, max_consumers = max_consumers)
        Consumer.__init__(self, producers = producers, max_producers = max_producers, has_parent_producer = has_parent_producer)

    def relay(self, packet):
        if packet.get_type() in [Packet.TYPE_QUERY]:
            Consumer.send(self, packet)
        elif packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            Producer.send(self, packet)

    send    = relay
    receive = relay
