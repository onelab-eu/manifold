#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Rename Node allows to rename field name(s) of
# input Records.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                  import StringTypes

from manifold.operators     import Node
from manifold.util.log      import Log 
from manifold.util.type     import returns

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

class Rename(Node):
    """
    RENAME operator node (cf SELECT clause in SQL)
    """

    def __init__(self, child, map_fields):
        """
        Constructor
        """
        super(Rename, self).__init__()
        self.child, self.map_fields = child, map_fields

        # Callbacks
        old_cb = child.get_callback()
        child.set_callback(self.child_callback)
        self.set_callback(old_cb)
        self.query = None

    @returns(dict)
    def get_map_fields(self):
        """
        Returns:
            A dictionnary {String : String} which maps the field name
            to rename with the corresponding updated field name.
        """
        return self.map_fields

    @returns(Node)
    def get_child(self):
        """
        Returns:
            A Node instance (the child Node) of this Rename instance.
        """
        return self.child

    def dump(self, indent = 0):
        """
        Dump the current node
        Args:
            indent: An integer corresponding to the current indentation
                in number of spaces.
        """
        Node.dump(self, indent)
        self.child.dump(indent + 1)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Node.
        """
        return DUMPSTR_RENAME % self.get_map_fields()

    def start(self):
        """
        Propagates a START message through this Node.
        """
        self.child.start()

    def child_callback(self, record):
        """
        Processes records received by the child node
        Args:
            record: A dictionary representing the received Record
        """
        if not record.is_last():
            #record = { self.map_fields.get(k, k): v for k, v in record.items() }
            try:
                for k, v in self.map_fields.items():
                    if k in record:
                        if '.' in v: # users.hrn
                            method, key = v.split('.')
                            if not method in record:
                                record[method] = list() 
                            for x in record[k]:
                                record[method].append({key: x})        
                        else:
                            record[v] = record.pop(k) #record[k]
                        #del record[k]
            except Exception, e:
                Log.error("Error in Rename::child_callback:", e)
                import traceback
                traceback.print_exc()
        self.send(record)

    @returns(Node)
    def optimize_selection(self, filter):
        """
        Propage WHERE operator through this RENAME Node.
        Args:
            filter: A Filter instance storing the WHERE clause.
        Returns:
            The update root Node of the optimized AST.
        """
        # TODO We must rename fields involved in filter
        Log.warning('Not implemented')
        return self

    @returns(Node)
    def optimize_projection(self, fields):
        """
        Propage SELECT operator through this RENAME Node.
        Args:
            fields: A list of String correspoding the SELECTed fields.
        Returns:
            The update root Node of the optimized AST.
        """
        # TODO We must rename fields involved in filter
        Log.warning('Not implemented')
        return self
