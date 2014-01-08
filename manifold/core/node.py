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
from types                        import StringTypes

from manifold.core.packet           import Packet
from manifold.util.type             import accepts, returns

class Node(object):
    """
    A processing node. Base object
    """

    last_identifier = 0

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor.
        """
        try:
            self._identifier
            #print "node already init"
            return
        except: pass

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
        sys.stdout.write("[%04d]%s" % (
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

    @returns(StringTypes)
    def format_node(self, indent = 0):
        """
        Args:
            ident: An integer corresponding to the current indentation.
        Returns:
            The '%s' representation of this Node
        """
        return "[%(id)04d]%(tab)s %(self)s" % {
            "id"   : self.get_identifier(),
            "tab"  : " " * indent,
            "self" : self
        }

    @returns(StringTypes)
    def format_backward_paths_rec(self, indent, res):
        """
        Format debug information to test the path(s) from this Producer
        towards the end-Consumer(s). This function ends this recursion.
        Args:
            ident: An integer corresponding to the current indentation.
            res: The String we're crafting (rec)
        """
        return "%(res)s%(self)s" % {
            "res"  : "%s\n" % res if res else "",
            "self" : self.format_node(indent + 2)
        }
