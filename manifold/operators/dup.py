#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Dup Node filters every incoming Record that have already traversed
# this Dup Node. It acts like "| uniq" in shell.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.query            import Query
from manifold.operators             import ChildCallback
from manifold.operators.operator    import Operator
from manifold.util.type             import returns

#------------------------------------------------------------------
# DUP node
#------------------------------------------------------------------

class Dup(Operator):

    def __init__(self, child, key):
        """
        Constructor.
        Args:
            child: A Node instance, child of this Dup Node.
            key: A Key instance.
        """
        #assert issubclass(Node, type(child)), "Invalid child %r (%r)" % (child, type(child))
        #assert isinstance(Key,  type(key)),   "Invalid key %r (%r)"   % (key,   type(key))
        super(Dup, self).__init__()

        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        self.child_results = set()

#DEPRECATED|    @returns(Query)
#DEPRECATED|    def get_query(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The Query representing the data produced by the nodes.
#DEPRECATED|        """
#DEPRECATED|        return Query(self.child.get_query())
#DEPRECATED|
#DEPRECATED|    def get_child(self):
#DEPRECATED|        """
#DEPRECATED|        Returns
#DEPRECATED|            A Node instance (the child Node) of this Demux instance.
#DEPRECATED|        """
#DEPRECATED|        return self.child

    def dump(self, indent = 0):
        """
        Dump the current node.
        Args:
            indent: current indentation.
        """
        self.tab(indent)
        print "DUP (built above %r)" % self.get_child()
        self.get_child().dump(indent + 1)

#DEPRECATED|    def start(self):
#DEPRECATED|        """
#DEPRECATED|        \brief Propagates a START message through the node
#DEPRECATED|        """
#DEPRECATED|        self.child.start()
#DEPRECATED|        self.status.started(0)
#DEPRECATED|
#DEPRECATED|    def child_callback(self, child_id, record):
#DEPRECATED|        """
#DEPRECATED|        \brief Processes records received by a child node
#DEPRECATED|        \param record dictionary representing the received record
#DEPRECATED|        """
#DEPRECATED|        assert child_id == 0
#DEPRECATED|
#DEPRECATED|        if record.is_last():
#DEPRECATED|            self.status.completed(child_id)
#DEPRECATED|            return
#DEPRECATED|
#DEPRECATED|        if record not in self.child_results:
#DEPRECATED|            self.child_results.add(record)
#DEPRECATED|            self.send(record)
#DEPRECATED|            return
