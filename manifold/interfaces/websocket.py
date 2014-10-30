#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import sys, json

from twisted.internet.protocol      import Protocol, Factory, ClientFactory
from twisted.protocols.basic        import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread

# pip install autobahn
from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory

from manifold.core.packet           import GET
from manifold.core.query            import Query

# We still need to integrate all improvements done to the TCP socket
# interface...

class MyServerProtocol(WebSocketServerProtocol):

    def onConnect(self, request):
        print("Client connecting: {0}".format(request.peer))
        self.factory.on_client_ready(self)

    def onOpen(self):
        print("WebSocket connection open.")
        #self.sendMessage(u"bienvenue".encode(), False) # packet)

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
            print("Text message received: {0}".format(payload.decode('utf8')))
            # We expect a JSON dict
            query_dict = json.loads(payload.decode('utf-8'))
            packet = GET()
            query = Query.from_dict(query_dict)
            packet.set_destination(query.get_destination())

            self.factory.receive(packet)

        ## echo back message verbatim
         #self.sendMessage(payload, isBinary)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

    def send_packet(self, packet):#
        print "send_packet", packet
        # We assume we only send records...
        msg = json.dumps(packet.get_dict())
        self.sendMessage(msg.encode(), False)
        print "sent packet"

DEFAULT_PORT = 9000

# Only for server

class WebSocketInterface(Interface, WebSocketServerFactory):
    """
    """
    __interface_type__ = 'websocket'
    protocol = MyServerProtocol

    def __init__(self, router, port = DEFAULT_PORT):
        Interface.__init__(self, router)

        WebSocketServerFactory.__init__(self, "ws://localhost:%s" % port, debug = False)

        # _client = None means interface is down
        self._client = None
        self._tx_buffer = list()

        # Received packets are sent back to the client
        _self = self
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                # To avoid this flow to be remembered
                packet.set_receiver(None)
                _self.send(packet)

        self._receiver = MyReceiver()

        ReactorThread().start_reactor()
        ReactorThread().listenTCP(port, self) # factory) #TCPSocketInterface(router))

    def terminate(self):
        ReactorThread().stop_reactor()

    def on_client_ready(self, client):
        _self = self

        self._client = client
        self.is_up()

        # Received packets are sent back to the client
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                _self.send(packet)

        self._receiver = MyReceiver()

        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            self._client.send_packet(full_packet)

    def send_impl(self, packet):
        print "send impl", packet
        if not self._client:
            print "not client"
            self._tx_buffer.append(packet)
        else:
            print "client", self._client
            self._client.send_packet(packet)

    # We really are a gateway !!! A gateway is a specialized Interface that
    # answers instead of transmitting.
    # 
    # And a client, a Router are also Interface's, cf LocalClient

    # from protocol
    # = when we receive a packet from outside
    def receive(self, packet):
        print "receive", packet
        packet.set_receiver(self._receiver)
        Interface.receive(self, packet)
