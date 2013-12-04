#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Node class may corresponds to:
# - a Manifold operator  (see manifold/operators)
# - a Manifold interface (see manifold/core/interface.py)
#   for instance a Manifold router.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import sys

from manifold.core.packet           import Packet
from manifold.util.type             import accepts, returns

class Node(object):
    """
    A processing node. Base object
    """

    last_identifier = 0
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|    # Static methods
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|    @staticmethod
#DEPRECATED|    def connect(consumer, producer):
#DEPRECATED|        consumer.set_producer(producer)
#DEPRECATED|        producer.set_consumer(consumer)


    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor.
        """
        Node.last_identifier += 1
        self._identifier = Node.last_identifier

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(int)
    def get_identifier(self):
        """
        Returns:
            The identifier of this Node.
        """
        return self._identifier

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def tab(self, indent):
        """
        Print 'indent' tabs.
        Args:
            indent: An integer corresponding to the current indentation (in
                number of spaces)
        """
        sys.stdout.write("[%04d] %s" % (
            self.get_identifier(),
            ' ' * 4 * indent
        ))

    def dump(self, indent = 0):
        """
        Dump the current Node.
        Args:
            indent: An integer corresponding to the current indentation (in
                number of spaces)
        """
        self.tab(indent)
        print "%r" % self

    def check_packet(self, packet):
        assert isinstance(packet, Packet), \
            "Invalid packet = %s (%s)" % (packet, type(packet))

    def check_receive(self, packet):
        """
        Check Node::receive() parameters. This method should be overloaded.
        """
        self.check_packet(packet)

    def check_send(self, packet):
        """
        Check Node::send() parameters. This method should be overloaded.
        """
        self.check_packet(packet)

    def send(self, packet):
        """
        (pure virtual method). Send a Packet.
        Args:
            packet: A Packet instance. 
        """
        self.check_send(packet)
        raise NotImplementedError("Method 'send' must be overloaded: %s" % self.__class__.__name__)
        
    def receive(self, packet):
        """
        (pure virtual method). Send a Packet.
        Args:
            packet: A Packet instance.
        """
        self.check_receive(packet)
        raise NotImplementedError("Method 'receive' must be overloaded: %s" % self.__class__.__name__)
