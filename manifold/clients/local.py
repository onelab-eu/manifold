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

import asynchat, errno, socket, threading, time, traceback

# Bugfix http://hg.python.org/cpython/rev/16bc59d37866 (FC14 or less)
# On recent linux distros we can directly "import asyncore"
#import manifold.util.asyncore as asyncore
import asyncore

from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.packet           import Packet, QueryPacket
from manifold.core.query            import Query
from manifold.core.result_value     import ResultValue
from manifold.core.router           import Router
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

from ..clients.client               import ManifoldClient

class State(object): pass

class ManifoldLocalClient(ManifoldClient, asynchat.async_chat):

    STATE_LENGTH = State()
    STATE_PACKET = State()

    def __init__(self, path = '/tmp/manifold'):
        """
        Constructor.
        """
        super(ManifoldLocalClient, self).__init__()
        asynchat.async_chat.__init__(self)

        # XXX We need to enforce authentication separately
        self.user = None

        self._path = path
        self._receiver = None

        self.data = ""

        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._request_fifo = [] # UNUSED ?
        self._receive_buffer = []

        # Prepare packet reception (DUP)
        self._pstate = self.STATE_LENGTH
        self.set_terminator (8)

        try:
            self.connect(self._path)
        except socket.error, e:
            error_code = e[0]
            if error_code == errno.ENOENT:
                Log.error("Invalid socket: [%s], did you run manifold-router?" % self._path)
            raise e

        # Start asyncore thread
        self._thread = threading.Thread(target = asyncore.loop, kwargs = {'timeout' : 1})
        self._thread.start()

    def terminate(self):
        self.close()
        self._thread.join()

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
        return Annotation({"user" : self.user})

    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        """
        if self.user:
            return "Shell using local account %s" % self.user["email"]
        else:
            return "Shell using no account"

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        return self.user

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
        if not annotation:
            annotation = Annotation()
        annotation |= self.get_annotation()

        self._receiver = SyncReceiver()
        packet = QueryPacket(query, annotation, receiver = self._receiver)

        packet_str = packet.serialize()
        self.push('%08x%s' % (len(packet_str), packet_str))

        # This code is blocking
        result_value = self._receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value

#DEPRECATED|    def handle_connect (self):
#DEPRECATED|        # Push a query
#DEPRECATED|        query = Query.get('ping').filter_by('destination', '==', '8.8.8.8')
#DEPRECATED|        annotation = Annotation()
#DEPRECATED|
#DEPRECATED|        packet = QueryPacket(query, annotation, None)
#DEPRECATED|
#DEPRECATED|        packet_str = packet.serialize()
#DEPRECATED|        self.push ('%08x%s' % (len(packet_str), packet_str))

    def close(self):
        asynchat.async_chat.close(self)

    def collect_incoming_data (self, data):
        self._receive_buffer.append (data)

    def found_terminator (self):
        self._receive_buffer, data = [], ''.join (self._receive_buffer)

        if self._pstate is self.STATE_LENGTH:
            packet_length = int(data, 16)
            self.set_terminator(packet_length)
            self._pstate = self.STATE_PACKET
        else:
            # We shoud wait until last record
            packet = Packet.deserialize(data)

            self._receiver.receive(packet)

            # Prepare for next packet
            self.set_terminator (8)
            self._pstate = self.STATE_LENGTH

try:
    asyncore.loop()
except asyncore.ExitNow, e:
    print e
