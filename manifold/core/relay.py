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

from types                  import StringTypes

from manifold.core.packet   import Packet
from manifold.core.producer import Producer
from manifold.core.consumer import Consumer
from manifold.util.type     import accepts, returns

class Relay(Producer, Consumer):
    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = None):
        """
        Constructor:
        Args:
            See Procuder::__init__
            See Consumer::__init__
        """
        Producer.__init__(self, consumers = consumers, max_consumers = max_consumers)
        Consumer.__init__(self, producers = producers, max_producers = max_producers, has_parent_producer = has_parent_producer)

    def check_relay(self, packet):
        """
        Check whether self and packet are well-formed to run "Relay::relay()" method.
        Args:
            packet: A Packet instance.
        """
        if packet.get_type() != Packet.TYPE_ERROR:
            assert self.get_num_producers() > 0, "No Producer set in %s: packet = %s" % (self, packet)
        assert self.get_num_consumers() > 0, "No Consumer set in %s: packet = %s" % (self, packet)

    def relay(self, packet):
        """
        Forward an incoming Packet either to its Consumer or to its Producers
        according to the nature of the Packet.
        Args:
            packet: A Packet instance.
        """
        self.check_relay(packet)
        if packet.get_type() in [Packet.TYPE_QUERY]:
            Consumer.send(self, packet)
        elif packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            Producer.send(self, packet)

    send    = relay
    receive = relay

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Consumer.
        """
        return "%s<[%s] Consumers: %s Producers: %s>" % (
            self.__class__.__name__,
            self.get_identifier(),
            self.format_consumer_ids(),
            self.format_producer_ids()
        )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Relay.
        """
        return self.__repr__() 

