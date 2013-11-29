#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Socket.
#
# A Socket is binded with exactly one Consumer
# and exactly one Producer. Socket bufferize
# packets flowing between its Consumer and its
# Producer.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                  import StringTypes

from manifold.core.consumer import Consumer
from manifold.core.relay    import Relay
from manifold.util.log      import Log
from manifold.util.type     import accepts, returns

class Socket(Relay):
    def __init__(self, consumer):
        """
        Constructor.
        Args:
            consumer: A Consumer instance.
        """
        Log.tmp(">>>>>>>>>>> Creating Socket with consumer = %s" % consumer)
        assert isinstance(consumer, Consumer),\
            "Invalid consumer = %s" % (consumer, type(consumer))
        Relay.__init__(self, consumers = [consumer], max_consumers = 1, max_producers = 1)

    def check_send(self):
        assert self.get_num_producers() == 1,\
            "Invalid number of Producers in %s (expected: 1, got: %s) " % (
                self,
                self.get_num_producers()
            )

    def check_receive(self):
        assert self.get_num_consumers() == 1,\
            "Invalid number of Consumers in %s (expected: 1, got: %s)" % (
                self,
                self.get_num_consumers()
            )

    def send(self, packet):
        """
        Send a Packet receive by this Socket toward its Producer (resp.
        its Consumer) according to the type of Packet.
        Args:
            packet: A Packet instance.
        """
        self.check_send()
        Log.tmp("--> [%s] sending %s" % (self.get_identifier(), packet))
        super(Socket, self).send(packet)

    def receive(self, packet):
        """
        Process and forward an incoming Packet receive by this Socket to
        the appropriate neighbor.
        Args:
            packet: A Packet instance.
        """
        self.check_receive()
        Log.tmp("<-- [%s] receiving %s" % (self.get_identifier(), packet))
        super(Socket, self).receive(packet)
    
