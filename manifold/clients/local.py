#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldLocalClient is used to perform query on
# a Manifold Router that we run locally.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import errno, socket, threading, time, traceback

from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.packet           import Packet, GET
from manifold.core.query            import Query
from manifold.core.result_value     import ResultValue
from manifold.core.router           import Router
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.constants        import SOCKET_PATH
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

from ..clients.client               import ManifoldClient

from manifold.interfaces.tcp_socket  import TCPClientInterface
from manifold.interfaces.unix_socket import UNIXClientInterface

class State(object): pass

class ManifoldLocalClient(ManifoldClient):

    def __init__(self, username, socket_path = SOCKET_PATH):
        """
        Constructor.
        Args:
            username: A String containing the user's login (usually its
                email address).
            socket_path: The path to the socket file corresponding to a
                running ManifoldRouter.
        """
        super(ManifoldLocalClient, self).__init__()
        self._receiver = self.make_receiver()

        self._interface = TCPClientInterface(self._receiver, None, host = 'localhost', up = False)
        #self._interface = UNIXClientInterface(self._receiver, {'filename': socket_path})
        self._interface.set_reconnecting(False)
        self._interface.set_up()
        self._user = None

    def terminate(self):
        self._interface.terminate()

    def make_receiver(self):
        receiver = SyncReceiver()
        receiver.register_interface = lambda x : None
        receiver.up_interface       = lambda x : None
        receiver.down_interface     = lambda x : None
        receiver.get_fib            = lambda   : None
        return receiver

    #--------------------------------------------------------------
    # Overloaded methods
    #--------------------------------------------------------------

    # XXX All authentication related stuff should be handled in the parent class
    # switching to session as soon as possible
    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation to pass to the QUERY Packet
            sent to the Router.
        """
        return Annotation({"user" : self._user})

    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        """
        if self._user:
            return "Shell using local account %s" % self._user["email"]
        else:
            return "Shell using no account"

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        return self._user

#DEPRECATED|    def send(self, packet):
#DEPRECATED|        """
#DEPRECATED|        Send a Packet to the nested Manifold Router.
#DEPRECATED|        Args:
#DEPRECATED|            packet: A QUERY Packet instance.
#DEPRECATED|        """
#DEPRECATED|        print "send", packet
#DEPRECATED|        assert isinstance(packet, Packet), \
#DEPRECATED|            "Invalid packet %s (%s)" % (packet, type(packet))
#DEPRECATED|        assert packet.get_protocol() == Packet.PROTOCOL_QUERY, \
#DEPRECATED|            "Invalid packet %s of type %s" % (
#DEPRECATED|                packet,
#DEPRECATED|                Packet.get_protocol_name(packet.get_protocol())
#DEPRECATED|            )
#DEPRECATED|        self.router.receive(packet)

    @returns(ResultValue)
    def forward(self, query, annotation = None):
        """
        Send a Query to the nested Manifold Router.
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """

        packet = Packet()
        packet.set_protocol(query.get_protocol())
        packet.set_protocol(query.get_protocol())
        data = query.get_data()
        if data:
            packet.set_data(data)

        packet.set_source(self._interface.get_address())
        packet.set_destination(query.get_destination())
        if annotation:
            packet.update_annotation(annotation)

        r = self.make_receiver()
        packet.set_receiver(r)

        self._interface.send(packet)

        # This code is blocking
        result_value = r.get_result_value()
        #result_value = self._receiver.get_result_value()

        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value
