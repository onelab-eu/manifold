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


#import random, sys
import sys
from manifold.util.type     import accepts, returns

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
