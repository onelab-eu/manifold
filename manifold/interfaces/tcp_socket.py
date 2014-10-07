#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread

DEFAULT_PORT = 50000

# Only for server

# A protocol is recreated everytime a connection is made
# so it cannot be an interface...
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

        self.factory.on_client_connected(self)


    # connection lost = client=None in factory

class TCPSocketFactory(Interface, Factory):
    """
    """
    protocol = ManifoldProtocol
    __interface_name__ = 'tcp'

    def __init__(self, router):
        Interface.__init__(self, router)
        self._tx_buffer = list()
        # _client = None means interface is down
        self._client = None

        # Received packets are sent back to the client
        _self = self
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                print "RECEIVED PACKET ON INTERFACE", _self
                _self._client.send(packet)

        self._receiver = MyReceiver()

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    def is_up(self):
        return self._client is not None

    def is_down(self):
        return not self.is_up()

    def on_client_connected(self, client):
        self._request_announces()

        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            client.send_packet(full_packet)

        self._client = client



    def send_impl(self, packet):
        if not self._client:
            self._tx_buffer.append(packet)
        else:
            self._client.send_packet(packet)

    # We really are a gateway !!! A gateway is a specialized Interface that
    # answers instead of transmitting.
    # 
    # And a client, a Router are also Interface's, cf LocalClient

    # from protocol
    # = when we receive a packet from outside
    def receive(self, packet):
        packet.set_receiver(self._receiver)
        Interface.receive(self, packet)

# This is in fact the TCPClientSocketInterface..
# This is a bit weird but we need this to return an interface, and twisted
# protocols do not allow this.
class TCPSocketInterface(TCPSocketFactory, ClientFactory):

    __interface_name__ = 'tcpclient'

    def __init__(self, router, host, port = DEFAULT_PORT):
        TCPSocketFactory.__init__(self, router)
        ReactorThread().connectTCP(host, port, self)
        print "||||| TCPClientSocketInterface", self

    def connect(self, host):
        # This should down, reconnect, then up the interface
        # raises Timeout, MaxConnections
        Log.warning("Connect not implemented")

# This interface will spawn other interfaces
class TCPServerSocketInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """

    __interface_name__ = 'tcpserver'

    def __init__(self, router, port = DEFAULT_PORT):
        Interface.__init__(self, router)
        ReactorThread().listenTCP(port, TCPSocketFactory(router))
        print "||||| TCPServerSocketInterface", self
        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()
