#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct

from twisted.internet.protocol import Protocol, ServerFactory, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread

DEFAULT_PORT = 50000

################################################################################
# BASE CLASSES
################################################################################

# A protocol is recreated everytime a connection is made
# so it cannot be an interface...
class ManifoldProtocol(IntNStringReceiver):
    """ 
    Base class for building client and server protocols.

    The protocol is based on twisted.protocols.basic IntNStringReceiver, with
    little-endian 32-bit length prefix.
    """

    structFormat = "<L"
    prefixLength = struct.calcsize(structFormat)

    def connectionMade(self):
        self.factory.on_client_connected(self)

    def stringReceived(self, msg):
        packet = Packet.deserialize(msg)
        self.receive(packet)

    def send_packet(self, packet):
        self.sendString(packet.serialize())

    def connectionLost(self, reason):
        print "CONNECTION LOST: REASON:", reason, " - CLIENT", self
        self.factory.on_client_disconnected(self, reason)
    # connection lost = client=None in factory


class TCPInterface(Interface):
    """
    Base class for building client and server interfaces.
    """

    __interface_type__ = 'socket'

    def __init__(self, router):
        Interface.__init__(self, router)
        self._tx_buffer = list()
        # _client = None means interface is down
        self._client = None

        # Received packets are sent back to the client
        _self = self
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                _self.send(packet)

        self._receiver = MyReceiver()

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    def send_impl(self, packet):
        raise NotImplemented # Should be overloaded in children classes

    # We really are a gateway !!! A gateway is a specialized Interface that
    # answers instead of transmitting.
    # 
    # And a client, a Router are also Interface's, cf LocalClient

    # from protocol
    # = when we receive a packet from outside
    def receive(self, packet):
        packet.set_receiver(self._receiver)
        Interface.receive(self, packet)

################################################################################
# PROTOCOLS
################################################################################

class ManifoldClientProtocol(ManifoldProtocol):

    # In the client, the interface is the factory. We forward the received
    # packet to the interface.
    def receive(self, packet):
        self.factory.receive(packet)

# For the server, the protocol is the interface
class ManifoldServerProtocol(ManifoldProtocol, TCPInterface):

    def send_impl(self, packet):
        self.send_packet(packet)

################################################################################
# FACTORIES
################################################################################

# For the client, the factory is the interface. We have a single client
# interface and it is maintained through the various connections and
# disconnections
class TCPClientSocketFactory(TCPInterface, ClientFactory):
    """
    """
    protocol = ManifoldClientProtocol

    def on_client_connected(self, client):
        self._client = client

        self.up()

        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            self.send(full_packet)

    def on_client_disconnected(self, client, reason):
        self._client = None

    def send_impl(self, packet):
        if self.is_down():
            self._tx_buffer.append(packet)
        else:
            self._client.send_packet(packet)



class TCPServerSocketFactory(ServerFactory):
    """
    """
    protocol = ManifoldServerProtocol

    def __init__(self, router):
        self._router = router

    def buildProtocol(self, addr):
        p = self.protocol(self._router)
        p.factory = self
        return p

    # We don't care about clients being connected, since we have spawned an
    # interface for this purpose
    def on_client_connected(self, client):
        pass

    def on_client_disconnected(self, client, reason):
        pass


################################################################################
# Interfaces
################################################################################


# This is in fact the TCPClientSocketInterface..
# This is a bit weird but we need this to return an interface, and twisted
# protocols do not allow this.
class TCPClientInterface(TCPClientSocketFactory):

    __interface_type__ = 'tcpclient'

    def __init__(self, router, host, port = DEFAULT_PORT):
        TCPClientSocketFactory.__init__(self, router)
        ReactorThread().connectTCP(host, port, self)

    def connect(self, host):
        # This should down, reconnect, then up the interface
        # raises Timeout, MaxConnections
        Log.warning("Connect not implemented")

# This interface will spawn other interfaces
class TCPServerInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """
    __interface_type__ = 'tcpserver'

    def __init__(self, router, port = DEFAULT_PORT):
        Interface.__init__(self, router)
        # We should create a new one each time !!!!
        ReactorThread().listenTCP(port, TCPServerSocketFactory(router))
        ReactorThread().start_reactor()
        self.up()

    def terminate(self):
        ReactorThread().stop_reactor()

    def _request_announces(self):
        pass
