#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Local router process reading on UNIX socket

import asyncore, socket, os
from manifold.core.operator_slot import ChildSlotMixin
from manifold.core.packet import Packet
from manifold.core.router import Router
from manifold.core.sync_receiver import SyncReceiver
from manifold.gateways import Gateway
import asynchat

class State(object): pass

class QueryHandler(asynchat.async_chat, ChildSlotMixin):

    STATE_LENGTH = State()
    STATE_PACKET = State()

    def __init__ (self, conn, addr, callback):
        asynchat.async_chat.__init__ (self, conn)
        ChildSlotMixin.__init__(self) # XXX
        self.addr = addr

        # Reading socket data is done in two steps: first get the length of the
        # packet, then read the packet of a given length
        self.pstate = self.STATE_LENGTH
        self.set_terminator(8)
        self._receive_buffer = []
        self.callback = callback


    def log (self, *items):
        print "log", self.__class__, items

    def collect_incoming_data (self, data):
        self._receive_buffer.append (data)

    def found_terminator (self):
        self._receive_buffer, data = [], ''.join(self._receive_buffer)

        if self.pstate is self.STATE_LENGTH:
            packet_length = int(data, 16)
            self.set_terminator(packet_length)
            self.pstate = self.STATE_PACKET
        else:
            self.set_terminator (8)
            self.pstate = self.STATE_LENGTH

            packet = Packet.deserialize(data)

            self.callback(self.addr, packet, receiver = self) or ""

    def receive(self, packet):
        packet_str = packet.serialize()
        self.push(('%08x' % len(packet_str)) + packet_str)

class RouterDaemon(asyncore.dispatcher):

    path = '/tmp/manifold'

    def __init__ (self):
        asyncore.dispatcher.__init__(self)

        # Router initialization
        self._router = Router()
        self._router.add_platform('ping', 'ping_process')

        self.create_socket (socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self.path)
        self.listen(128)


    def handle_accept (self):
        conn, addr = self.accept()
        QueryHandler(conn, addr, self.on_received)

    def on_received(self, addr, packet, receiver):
        packet.set_receiver(receiver)
        self._router.receive(packet)

Gateway.register_all()
server = RouterDaemon()
try:
    asyncore.loop()
finally:
    if os.path.exists(RouterDaemon.path):
        os.unlink(RouterDaemon.path)
