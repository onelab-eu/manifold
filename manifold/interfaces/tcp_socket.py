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



class TCPSocketInterface(Factory, Interface):
#class TCPSocketInterface(Interface, ClientFactory):
    """
    """
    protocol = ManifoldProtocol

    def __init__(self, router):
        Interface.__init__(self, router)
        self._client    = None
        self._tx_buffer = list()
        self._receiver = None
        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    def on_client_ready(self, client):
        _self = self

        self._request_announces()
        self._client = client
        # Received packets are sent back to the client
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                _self.send(packet)

        self._receiver = MyReceiver()

        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            self._client.send_packet(full_packet)

    def send_impl(self, packet, destination, receiver):
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

class TCPClientSocketInterface(ClientFactory, TCPSocketInterface):
    """
    A client is actively connecting a server and has the initiative to reconnect
    """
    __interface_name__ = 'tcp'

    def __init__(self, router, host, port = DEFAULT_PORT):
        TCPSocketInterface.__init__(self, router)
        ReactorThread().connectTCP(host, port, self)

    def connect(self, host):
        # This should down, reconnect, then up the interface
        # raises Timeout, MaxConnections
        Log.warning("Connect not implemented")

class TCPServerSocketInterface(TCPSocketInterface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """

    __interface_name__ = 'tcpserver'

    def __init__(self, router, port = DEFAULT_PORT):
        TCPSocketInterface.__init__(self, router)
        ReactorThread().listenTCP(port, TCPSocketInterface(router))
