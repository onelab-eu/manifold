#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct, uuid

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.core.annotation       import Annotation
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.interface        import Interface
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet, GET
from manifold.core.query            import Query
from manifold.util.log              import Log
from manifold.util.predicate        import Predicate
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
        self.factory.receive(packet)

    def send_packet(self, packet):
        self.sendString(packet.serialize())

    def connectionMade(self):
        self.factory.on_client_ready(self)

class ManifoldServerProtocol(ManifoldProtocol):
    def stringReceived(self, msg):
        packet = Packet.deserialize(msg)
        self.factory.orig_receive(packet)

class ManifoldServerFactory(Factory, Interface, ChildSlotMixin):
    protocol = ManifoldServerProtocol

    def __init__(self, router):
        ChildSlotMixin.__init__(self) # needed to act as a receiver (?)
        self._router    = router
        self._client    = None
        self._receiver_map = dict()

    def on_client_ready(self, client):
        self._client = client

    def orig_receive(self, packet):
        print "SERVER RECEIVED PACKET FROM CLIENT", packet
        packet.set_receiver(self)
        # XXX The receiver is a UUID, let's preserve it
        self._router.receive(packet)

    # This is the receive function from the gateway, and the receive function
    # from the receiver (packets from the router).
    # It should be a send (to the client)
    def receive(self, packet):
        print "SERVER SENDING PACKET TO CLIENT", packet
        self._client.send_packet(packet)

# XXX At the moment, it is similar to the server... let's see

# XXX Behaves like a gateway. A gateway is a node/an interface (?) that has
# static announces (not OML???).
class ManifoldClientFactory(ClientFactory, Interface, ChildSlotMixin): # Node
    protocol = ManifoldProtocol
    
    def __init__(self, router):
        print "manifold client initialized"
        ChildSlotMixin.__init__(self)
        self._router    = router
        self._client    = None
        self._tx_buffer = list()

        # XXX In all interfaces
        self._uuid      = str(uuid.uuid4())
        self._receiver_map = dict()

    def on_client_ready(self, client):
        print "manifold client ready. requesting announces"
        self.send(GET(), Destination('local:object'))
        print "announces req'ed"

        self._client = client

        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            print "sending full packet from buffer"
            self._client.send_packet(full_packet)
            print "full ok"

        # I behave as a Manifold gateway !!

    def receive(self, packet):
        """
        For packets received from the remote server."
        """
        print "CLIENT RECEIVED PACKET FROM SERVER", packet
        # XXX Not all packets are targeted at the router.
        # - announces are
        # - supernodes are not (they could eventually pass through the router)

        d = packet.get_destination()
        if not d in self._receiver_map:
            Log.warning("Default: send packet to router")
            self._router.receive(packet)
        else:
            self._receiver_map[d].receive(packet)

    def send(self, packet, destination = None, receiver = None):
        """
        Receive handler for packets arriving from the router.
        For packets coming from the client, directly use the router which is
        itself a receiver.
        """
        print "send => dest=", destination
        # XXX This code should be shared by all interfaces
        source = Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._uuid)))
        packet.set_source(source)

        if destination:
            packet.set_destination(destination)

        if receiver:
            receiver_id = str(uuid.uuid4())
            self._receiver_map[receiver_id] = receiver
            packet.set_receiver(receiver_id)

        print "CLIENT SENT PACKET TO SERVER", packet
        if not self._client:
            self._tx_buffer.append(packet)
        else:
            self._client.send_packet(packet)

    def disconnect(self):
        self.close()

class TCPSocketInterface(Interface):

    def __init__(self, router):
        Interface.__init__(self, router)
        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    def listen(self):
        ReactorThread().listenTCP(SERVER_PORT, ManifoldServerFactory(self._router))
        # We need to know all clients !

    # XXX An interface should connect to a single remote host
    def connect(self, host):
        factory = ManifoldClientFactory(self._router)
        ReactorThread().connectTCP(host, SERVER_PORT, factory)
        return factory

    # We really are a gateway !!! A gateway is a specialized Interface that
    # answers instead of transmitting.
    # 
    # And a client, a Router are also Interface's, cf LocalClient
