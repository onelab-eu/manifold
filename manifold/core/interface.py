#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct

from twisted.internet.protocol import Protocol, Factory #, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.core.annotation       import Annotation
from manifold.core.query            import Query
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet, QueryPacket
from manifold.util.reactor_thread   import ReactorThread

SERVER_HOST = 'localhost'
SERVER_PORT = 50000

# Only for server

class ManifoldProtocol(IntNStringReceiver):
    """ The protocol is based on twisted.protocols.basic
        IntNStringReceiver, with little-endian 32-bit
        length prefix.
    """
    structFormat = "<L"
    prefixLength = struct.calcsize(structFormat)

    def stringReceived(self, msg):
        packet = Packet.deserialize(msg)
        self.factory.on_packet_received(packet)

    def send_packet(self, packet):
        self.sendString(packet.serialize())

    def connectionMade(self):
        self.factory.on_client_ready(self)

class ManifoldServerFactory(Factory, ChildSlotMixin):
    protocol = ManifoldProtocol

    def __init__(self, router):
        ChildSlotMixin.__init__(self)
        self._router = router

    def on_client_ready(self, client):
        self._client = client

    def on_packet_received(self, packet):
        packet.set_receiver(self)
        self._router.receive(packet)

    def receive(self, packet):
        self._client.send_packet(packet)

# XXX At the moment, it is similar to the server... let's see

class ManifoldClientFactory(Factory, ChildSlotMixin):
    protocol = ManifoldProtocol
    
    def __init__(self, router):
        ChildSlotMixin.__init__(self)
        self._router = router

    def on_client_ready(self, client):
        self._client = client

    def on_packet_received(self, packet):
        packet.set_receiver(self)
        self._router.receive(packet)

    def receive(self, packet):
        self._client.send_packet(packet)

# XXX We need to keep track of clients to propagate announces

class Interface(object):

    def __init__(self, router):
        self._router = router
        ReactorThread().start_reactor()

    def listen(self):
        ReactorThread().listenTCP(SERVER_PORT, ManifoldServerFactory(self._router))

#    def connect(self, host):
#        client = None # ClientFactory
#        ReactorThread().connectTCP(host, SERVER_PORT, client)

    def terminate(self):
        ReactorThread().stop_reactor()
