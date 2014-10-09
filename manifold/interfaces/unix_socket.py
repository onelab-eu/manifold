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
from manifold.interfaces.tcp_socket import TCPClientSocketFactory, TCPServerSocketFactory

DEFAULT_FILENAME = '/var/run/manifold/manifold.sock'

class UNIXClientInterface(TCPClientSocketFactory):

    __interface_tyfpe__ = 'unixclient'

    def __init__(self, router, filename = DEFAULT_FILENAME):
        TCPClientSocketFactory.__init__(self, router)
        ReactorThread().connectUNIX(filename, self)

    def connect(self, host):
        # This should down, reconnect, then up the interface
        # raises Timeout, MaxConnections
        Log.warning("Connect not implemented")

# This interface will spawn other interfaces
class UNIXServerInterface(Interface):

    __interface_type__ = 'unixserver'

    def __init__(self, router, filename = DEFAULT_FILENAME):
        Interface.__init__(self, router)
        # We should create a new one each time !!!!
        ReactorThread().listenUNIX(filename, TCPServerSocketFactory(router))
        ReactorThread().start_reactor()
        self.up()

    def _request_announces(self):
        pass
