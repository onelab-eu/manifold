#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An Union Node aggregates the Record returned by several
# child Nodes.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.projection  import Projection
from manifold.operators.operator    import Operator
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

DUMPSTR_UNION = "UNION"

#------------------------------------------------------------------
# UNION node
#------------------------------------------------------------------
            
class Union(Operator):
    """
    UNION operator node.
    """

    def __init__(self, children, key, distinct=True):
        """
        Constructor.
        Args:
            children: A list of Node instances, the children of
                this Union Node.
            key: A Key instance, corresponding to the key for
                elements returned from the node.
        """
        super(Union, self).__init__()
        # Parameters
        self.children, self.key = children, key
        # Member variables
        #self.child_status = 0
        #self.child_results = {}
        # Stores the list of keys already received to implement DISTINCT
        self.distinct = distinct
        self.key_list = []
        self.status = ChildStatus(self.all_done)
        # Set up callbacks
        for i, child in enumerate(self.children):
            child.set_callback(ChildCallback(self, i))

        # We suppose all children have the same format...
        # NOTE: copy is important otherwise we use the same
        self.query = self.children[0].get_query().copy()


    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # We suppose all child queries have the same format, and that we have at
        # least one child
        print "Union::get_query()"
        return Query(self.children[0].get_query())
        
    def dump(self, indent = 0):
        """
        Dump the this Union instance to the standard output. 
        Args:
            indent: An integer corresponding to the number of spaces
                to write (current indentation).
        """
        super(Union, self).dump(indent)
        for child in self.children:
            child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_UNION

    def start(self):
        """
        Propagates a START message through the Node.
        """
        # Start all children
        for i, child in enumerate(self.children):
            self.status.started(i)
        for i, child in enumerate(self.children):
            child.start()

    def inject(self, records, key, query):
        """
        Inject Record / record keys into the Node
        Args:
            records: A list of dictionaries representing Records,
                or list of Record keys
        """
        for i, child in enumerate(self.children):
            self.children[i] = child.inject(records, key, query)
        return self

    def all_done(self):
        #for record in self.child_results.values():
        #    self.send(record)
        self.send(Record(last = True))

    def child_callback(self, child_id, record):
        """
        Processes records received by the child Node.
        Args:
            child_id: identifier of the child that received the Record.
            record: dictionary representing the received Record.
        """
        if record.is_last():
            self.status.completed(child_id)
            return
        
        key = self.key.get_field_names()

        # DISTINCT not implemented, just forward the record
        if not key:
            Log.critical("No key associated to UNION operator")
            self.send(record)
            return

        # Ignore records that have no key
        if not Record.has_fields(record, key):
            Log.warning("UNION ignored record without key '%(key)s': %(record)r", **locals())
            return

        # Ignore duplicate records
        if self.distinct:
            key_value = Record.get_value(record, key)
            if key_value in self.key_list:
                Log.warning("UNION ignored duplicate record: %r" % record)
                return
            self.key_list.append(key_value)

        self.send(record)

        # XXX This code was necessary at some point to merge records... let's
        # keep it for a while
        #
        #    # Merge ! Fields must be the same, subfield sets are joined
        #    previous = self.child_results[record[self.key]]
        #    for k,v in record.items():
        #        if not k in previous:
        #            previous[k] = v
        #            continue
        #        if isinstance(v, list):
        #            previous[k].extend(v)
        #        else:
        #            if not v == previous[k]:
        #                print "W: ignored conflictual field"
        #            # else: nothing to do
        #else:
        #    self.child_results[record[self.key]] = record

#DEPRECATED#    def optimize(self):
#DEPRECATED#        for i, child in enumerate(self.children):
#DEPRECATED#            self.children[i] = child.optimize()
#DEPRECATED#        return self

    def optimize_selection(self, filter):
        # UNION: apply selection to all children
        for i, child in enumerate(self.children):
            old_child_callback= child.get_callback()
            self.children[i] = child.optimize_selection(filter)
            self.children[i].set_callback(old_child_callback)
        return self

    def optimize_projection(self, fields):
        # UNION: apply projection to all children
        # XXX in case of UNION with duplicate elimination, we need the key
        # until then, apply projection to all children
        #self.query.fields = fields
        do_parent_projection = False
        if self.distinct:
            key = self.key.get_field_names()
            if key not in fields: # we are not keeping the key
                do_parent_projection = True
                child_fields  = set()
                child_fields |= fields
                child_fields |= key
        for i, child in enumerate(self.children):
            old_child_callback= child.get_callback()
            self.children[i] = child.optimize_projection(child_fields)
            self.children[i].set_callback(old_child_callback)
        if do_parent_projection:
            old_self_callback = self.get_callback()
            projection = Projection(self, fields)
            projection.set_callback(old_self_callback)
            return projection
        return self
