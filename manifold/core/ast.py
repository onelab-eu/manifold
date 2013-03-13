#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Abstract Syntax Tree: 
#   An AST represents a query plan. It is made of a set
#   of interconnected Node instance which may be an SQL
#   operator (SELECT, FROM, UNION, LEFT JOIN, ...).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys
from copy                       import copy, deepcopy

from types                      import StringTypes
from manifold.core.filter       import Filter, Predicate
from manifold.core.query        import Query
from manifold.core.table        import Table 
from manifold.core.field        import Field
from manifold.core.key          import Key
from manifold.util.type         import returns, accepts
from manifold.util.log          import *

# NOTES
# - What about a Record type
# - What about subqueries in records ?

#------------------------------------------------------------------
# Constants used for dumping AST nodes
#------------------------------------------------------------------

DUMPSTR_FROM       = "SELECT %s FROM %s::%s" 
DUMPSTR_FROMTABLE  = "SELECT %s FROM [%r, ...]" 
DUMPSTR_PROJECTION = "SELECT [%s]" 
DUMPSTR_SELECTION  = "WHERE %s"
DUMPSTR_UNION      = "UNION"
DUMPSTR_SUBQUERIES = "<subqueries>"

#------------------------------------------------------------------
# Other constants
#------------------------------------------------------------------

# LAST_RECORD marker
# This constant is returned when a AST node has finished to return
# the records it provides.
LAST_RECORD = None

#------------------------------------------------------------------
# Utility classes
#------------------------------------------------------------------

class ChildCallback:
    """
    Implements a child callback function able to store its identifier.
    """

    def __init__(self, parent, child_id):
        """
        Constructor
        \param parent Reference to the parent class
        \param child_id Identifier of the children
        """
        self.parent, self.child_id = parent, child_id

    def __call__(self, record):
        """
        \brief Process records received by the callback
        """
        # Pass the record to the parent with the right child identifier
        self.parent.child_callback(self.child_id, record)

#------------------------------------------------------------------
# ChildStatus
#------------------------------------------------------------------

class ChildStatus:
    """
    Monitor child completion status
    """

    def __init__(self, all_done_cb):
        """
        \brief Constructor
        \param all_done_cb Callback to raise when are children are completed
        """
        self.all_done_cb = all_done_cb
        self.counter = 0

    def started(self, child_id):
        """
        \brief Call this function to signal that a child has completed
        \param child_id The integer identifying a given child node
        """
        self.counter += child_id + 1

    def completed(self, child_id):
        """
        \brief Call this function to signal that a child has completed
        \param child_id The integer identifying a given child node
        """
        self.status.child_status -= child_id + 1
        assert self.child_status >= 0, "Child status error: %d" % self.child_status
        if self.child_status == 0:
            self.all_done_cb()

#------------------------------------------------------------------
# Shared utility function
#------------------------------------------------------------------

def do_projection(record, fields):
    """
    Take the necessary fields in dic
    """
    ret = {}

    # 1/ split subqueries
    local = []
    subqueries = {}
    for f in fields:
        if '.' in f:
            method, subfield = f.split('.', 1)
            if not method in subqueries:
                subqueries[method] = []
            subqueries[method].append(subfield)
        else:
            local.append(f)
    
    # 2/ process local fields
    for l in local:
        ret[l] = record[l] if l in record else None

    # 3/ recursively process subqueries
    for method, subfields in subqueries.items():
        # record[method] is an array whose all elements must be
        # filtered according to subfields
        arr = []
        for x in record[method]:
            arr.append(local_projection(x, subfields))
        ret[method] = arr

    return ret

#------------------------------------------------------------------
# Node (parent class)
#------------------------------------------------------------------

class Node(object):
    """
    \brief Base class for implementing AST node objects
    """

    def __init__(self, node = None):
        """
        \brief Constructor
        """
        if node:
            if not isinstance(node, Node):
                raise ValueError('Expected type Node, got %s' % node.__class__.__name__)
            return deepcopy(node)
        # Callback triggered when the current node produces data.
        self.callback = None
        # Query representing the data produced by the node.
#        self.query = self.get_query()

    def set_callback(self, callback):
        """
        \brief Associates a callback to the current node
        \param callback The callback to associate
        """
        self.callback = callback
        # Basic information about the data provided by a node

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """

        # Raise an exception in the base class to force child classes to
        # implement this method
        raise Exception, "Nodes should implement the query function: %s" % self.__class__.__name__

    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        sys.stdout.write(' ' * indent * 4)

#------------------------------------------------------------------
# FROM node
#------------------------------------------------------------------

class From(Node):
    """
    \brief FROM node
    From Node are responsible to query a gateway (!= FromTable).
    """

    def __init__(self, table, query):
        """
        \brief Constructor
        \param table A Table instance (the 3nf table)
            \sa manifold.core.table.py
        \param query A Query instance: the query passed to the gateway to fetch records 
        """
        assert isinstance(query, Query), "Invalid type: query = %r (%r)" % (query, type(query))
        assert isinstance(table, Table), "Invalid type: table = %r (%r)" % (table, type(table))

        self.query, self.table = query, table
        super(From, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # The query returned by a FROM node is exactly the one that was
        # requested
        return self.query

    #@returns(Table)
    def get_table(self):
        """
        \return The Table instance queried by this FROM node.
        """
        return self.table

    @returns(StringTypes)
    def get_platform(self):
        """
        \return The name of the platform queried by this FROM node.
        """
        return list(self.get_table().get_platforms())[0]

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print DUMPSTR_FROM % (
            ', '.join(self.get_query().get_select()),
            self.get_platform(),
            self.get_query().get_from()
        )

    #@returns(From)
    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records,
                       or list of record keys
        \return This node
        """
        if not records:
            return
        record = records[0]

        # Are the records a list of full records, or only record keys
        is_record = isinstance(record, dict)

        # If the injection does not provides all needed fields, we need to
        # request them and join
        provided_fields = set(record.keys()) if is_record else set([self.key])
        needed_fields = self.query.fields
        missing_fields = needed_fields - provided_fields

        if not missing_fields:
            return FromTable(self.query, records)

        missing_fields |= self.key
        self.query.fields = missing_fields

        parent_query = Query(self.query)
        parent_query.fields = provided_fields
        parent_from = FromTable(parent_query, records)

        old_self_callback = self.callback
        join = LeftJoin(parent_from, self, self.key)
        join.callback = old_self_callback

        return join

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        if not self.gateway:
            raise Exception, "Cannot call start on a From class, expecting Gateway"
        self.gateway.start()

    def set_gateway(self, gateway):
        gateway.set_callback(self.callback)
        self.gateway = gateway

    def set_callback(self, callback):
        super(From, self).set_callback(callback)
        if self.gateway:
            self.gateway.set_callback(callback)

class FromTable(From):
    """
    \brief FROM node querying a list of records
    """

    def __init__(self, query, records = []):
        """
        \brief Constructor
        \param query A Query instance
        \param records A list of records (dictionnary instances)
        """
        assert isinstance(query,   Query), "Invalid query = %r (%r)"   % (query,   type(query))
        assert isinstance(records, list),  "Invalid records = %r (%r)" % (records, type(records))

        self.query, self.records = query, records
        super(FromTable, self).__init__()

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print DUMPSTR_FROMTABLE % (
            ', '.join(self.get_query().get_select()),
            self.records[0]
        )

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        for record in self.records:
            self.callback(record)
        self.callback(LAST_RECORD)

#------------------------------------------------------------------
# LEFT JOIN node
#------------------------------------------------------------------

class LeftJoin(Node):
    """
    LEFT JOIN operator node
    """

    @staticmethod
    def check_init(left_child, right_child, predicate, callback):
        assert issubclass(type(left_child),  Node), "Invalid left child = %r (%r)"  % (left_child,  type(left_child))
        assert issubclass(type(right_child), Node), "Invalid right child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(predicate))

    def __init__(self, left_child, right_child, predicate, callback):
        """
        \brief Constructor
        \param left_child  A Node instance corresponding to left  operand of the LEFT JOIN
        \param right_child A Node instance corresponding to right operand of the LEFT JOIN
        \param predicate A Predicate instance invoked to determine whether two record of
            left_child and right_child can be joined.
        \param callback The callback invoked when the LeftJoin instance returns records. 
        """
        # Check parameters
        LeftJoin.check_init(left_child, right_child, predicate, callback)

        # Initialization
        self.left      = left_child
        self.right     = right_child 
        self.predicate = predicate
        self.callback  = callback

        # Set up callbacks
        self.status = ChildStatus(self.all_done)
        for i, child in enumerate(self.get_children()):
            child.callback = ChildCallback(self, i)
            self.status.started(i)

        super(LeftJoin, self).__init__()

    def get_children(self):
        return [self.left, self.right]

    def all_done(self):
        print "LeftJoin::all_done: not yet implemented"
        pass

    @returns(Query)
    def get_query(self):
        """
        \return The query representing AST reprensenting the AST rooted
            at this node.
        """
        q = Query(self.get_children()[0])
        for child in self.get_children():
            # XXX can we join on filtered lists ? I'm not sure !!!
            # XXX some asserts needed
            q.filters |= child.filters
            q.fields  |= child.fields
        return q
        
    #@returns(LeftJoin)
    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records A list of dictionaries representing records,
                       or a list of record keys
        \returns This node
        """
        if not records:
            return
        record = records[0]

        # Are the records a list of full records, or only record keys
        is_record = isinstance(record, dict)

        if is_record:
            records_inj = []
            for record in records:
                proj = do_projection(record, self.left.query.fields)
                records_inj.append(proj)
            self.left = self.left.inject(records_inj)
            # TODO injection in the right branch: only the subset of fields
            # of the right branch
            return self

        # TODO Currently we support injection in the left branch only
        # Injection makes sense in the right branch if it has the same key
        # as the left branch.
        self.left = self.left.inject(records)
        return self

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        node = self.right if self.left_done else self.left
        node.start()

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "JOIN", self.predicate
        if self.left:
            self.left.dump(indent + 1)
        else:
            print '[DATA]', self.left_results
        self.right.dump(indent + 1)

    def left_callback(self, record):
        """
        \brief Process records received by the left child
        \param record A dictionary representing the received record 
        """

        if record == LAST_RECORD:
            # Inject the keys from left records in the right child...
            self.right.inject(self.left_map.keys())
            # ... and start the right node
            self.right.start()
            return

        # Directly send records missing information necessary to join
        if self.predicate not in record or not record[self.predicate]:
            print "W: Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                    (self.predicate, record)
            self.callback(record)

        # Store the result in a hash for joining later
        self.left_map[record[self.predicate]] = record

    def right_callback(self, record):
        """
        \brief Process records received by the right child
        \param record A dictionary representing the received record 
        """

        if record == LAST_RECORD:
            # Send records in left_results that have not been joined...
            for leftrecord in self.left_results:
                self.callback(leftrecord)
            # ... and terminates
            self.callback(LAST_RECORD)
            return

        # Skip records missing information necessary to join
        if self.predicate not in record or not record[self.predicate]:
            print "W: Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                    (self.predicate, record)
            return
        
        key = record[self.predicate]
        # We expect to receive information about keys we asked, and only these,
        # so we are confident the key exists in the map
        # XXX Dangers of duplicates ?
        left_record = self.left_map[key]
        left_record.update(record)
        self.callback(left_record)

        del self.left_map[key]

#------------------------------------------------------------------
# PROJECTION node
#------------------------------------------------------------------

class Projection(Node):
    """
    PROJECTION operator node (cf SELECT clause in SQL)
    """

    def __init__(self, child, fields):
        """
        \brief Constructor
        \param child A Node instance which will be the child of
            this Node.
        \param fields A list of String corresponding of field names
            we're selecting.
        """
        self.child, self.fields = child, fields
        self.child.callback = self.callback
        super(Projection, self).__init__()

    @returns(list)
    def get_fields(self):
        """
        \returns The list of field names selected in this node.
        """
        return self.fields

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return The Query representing the data produced by the nodes.
        """
        q = Query(self.child.get_query())

        # Projection restricts the set of available fields (intersection)
        q.fields &= fields
        return q

    def dump(self, indent):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print DUMPSTR_PROJECTION % self.fields
        self.child.dump(indent+1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()

    #@returns(Projection)
    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records A list of dictionaries representing records,
                       or a list of record keys
        \return This node
        """
        self.child = self.child.inject(records)
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD:
            record = do_projection(record)
        self.callback(record)

#------------------------------------------------------------------
# Selection node (WHERE)
#------------------------------------------------------------------

class Selection(Node):
    """
    Selection operator node (cf WHERE clause in SQL)
    """

    def __init__(self, child, filters):
        """
        \brief Constructor
        \param child A Node instance (the child of this Node)
        \param filters A set of Predicate instances
        """
        assert issubclass(type(child), Node), "Invalid child = %r (%r)"   % (child,   type(child))
        assert isinstance(filters, set),      "Invalid filters = %r (%r)" % (filters, type(filters))

        self.child, self.filters = child, filters
        self.child.callback = self.callback
        super(Selection, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the childs.
        \return query representing the data produced by the childs.
        """
        q = Query(self.child.get_query())

        # Selection add filters (union)
        q.filters |= filters
        return q

    def dump(self, indent = 0):
        """
        \brief Dump the current child
        \param indent The current indentation
        """
        self.tab(indent)
        print DUMPSTR_SELECTION % self.filters
        self.child.dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the child
        """
        self.child.start()

    #@returns(Selection)
    def inject(self, records):
        """
        \brief Inject record / record keys into the child
        \param records A list of dictionaries representing records,
                       or list of record keys
        \return This node
        """
        self.child = self.child.inject(records)
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node 
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD or (self.filters and self.filters.match(record)):
            self.callback(record)
        #if record != LAST_RECORD and self.filters:
        #    record = self.filters.filter(record)
        #self.callback(record)

#------------------------------------------------------------------
# UNION node
#------------------------------------------------------------------
            
class Union(Node):
    """
    UNION operator node
    """

    def __init__(self, children, key):
        """
        Constructor
        \param children
        \param key the key for elements returned from the node
        """
        # Parameters
        self.children, self.key = children, key
        # Member variables
        #self.child_status = 0
        #self.child_results = {}
        # Stores the list of keys already received to implement DISTINCT
        self.key_list = []
        self.status = ChildStatus(self.all_done)
        # Set up callbacks
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)

        super(Union, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # We suppose all child queries have the same format, and that we have at
        # least one child
        return Query(self.children[0].query)
        
    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print DUMPSTR_UNION
        for child in self.children:
            child.dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start all children
        for i, child in enumerate(self.children):
            child.start()
            self.status.started(i)

    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
        """
        for i, child in enumerate(self.children):
            self.children[i] = child.inject(records)
        return self

    def all_done(self):
        for record in self.child_results.values():
            self.callback(record)
        self.callback(LAST_RECORD)

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by the child node
        \param child_id identifier of the child that received the record
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            self.status.completed(child_id)
            return
        # DISTINCT not implemented, just forward the record
        if not self.key:
            self.callback(record)
            return
        # Ignore records that have no key
        if self.key not in record:
            print "W: UNION ignored record without key"
            return
        # Ignore duplicate records
        if record[self.key] in self.key_list:
            print "W: UNION ignored duplicate record"
            return
        self.callback(record)

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

#------------------------------------------------------------------
# SUBQUERY node
#------------------------------------------------------------------

class SubQuery(Node):
    """
    SUBQUERY operator (cf nested SELECT statements in SQL)
    """

    def __init__(self, parent, children, key):
        """
        Constructor
        \param parent
        \param children
        \param key the key for elements returned from the node
        """
        # Parameters
        self.parent, self.children, self.key = parent, children, key

        # Member variables
        self.parent_output = []

        # Set up callbacks
        parent.callback = self.parent_callback

        # Prepare array for storing results from children: parent result can
        # only be propagated once all children have replied
        self.child_results = []
        self.status = ChildStatus(self.all_done)
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)
            self.child_results.append([])

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # Query is unchanged XXX ???
        return Query(self.parent.get_query())

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.parent.dump(indent+1)
        if not self.children: return
        self.tab(indent)
        print DUMPSTR_SUBQUERIES
        for child in self.children:
            child.dump(indent+1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start the parent first
        self.parent.start()
        print "Il y a peut etre un bug ici :)"
        self.status.started(0)

    def parent_callback(self, record):
        """
        \brief Processes records received by the parent node
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            # When we have received all parent records, we can run children
            if self.parent_output:
                self.run_children()
            return
        # Store the record for later...
        self.parent_output.append(record)

    def run_children(self):
        """
        \brief Modify children queries to take the keys returned by the parent into account
        """
        # Loop through children and inject the appropriate parent results
        for i, child in enumerate(self.children):
            # The parent_output should already have either a set of keys or a
            # set of records. They will serve an input for the JOINS
            # keys = filters for the left part of the first JOIN
            # records = (partial) replacement for the first JOIN
            #     we can request less and make a JOIN with existing data

            # Collect the list of all child records in order to make a single
            # child query; we will have to dispatch the results
            child_record_all = []
            child_keys_all = []
            only_key = False # We have only the key in child records
            for parent_record in self.parent_output:
                # XXX We are supposing 1..N here
                if not isinstance(parent_record[child.query.fact_table], list):
                    raise Exception, "run_children received 1..1 record; 1..N implemented only."
                for child_record in parent_record[child.query.fact_table]:
                    if isinstance(child_record, dict):
                        if not self.key in child_record:
                            # XXX case for links in slice.resource
                            continue
                        if not child_record[self.key] in child_keys_all:
                            child_record_all.append(child_record)
                            child_keys_all.append(child_record[self.key])
                        # else duplicate
                    else:
                        # XXX We should be injecting keys only what is common
                        # between all records. For this we should be remembering
                        # the list of child_record fields that are in common in
                        # all child records.
                        only_key = True
                        child_keys_all.append(child_record)
            
            # child is the AST receiving the injection
            if only_key:
                self.children[i] = child.inject(child_keys_all)
            else:
                self.children[i] = child.inject(child_record_all)

        # We make another loop since the children might have been modified in
        # the previous one.
        for i, child in enumerate(self.children):
            child.start()
            self.status.started(i)

    def all_done(self):
        """
        \brief Called when all children are done: processes results stored in
        the parent.
        """
        for o in self.parent_output:
            # Dispatching child results
            for i, child in enumerate(self.children):
                for record in o[child.query.fact_table]:
                    # Find the corresponding record in child_results and
                    # update the one in the parent with it
                    if self.key in record:
                        for r in self.child_results[i]:
                            if self.key in r and r[self.key] == record[self.key]:
                                record.update(r)
                    # XXX We ignore missing keys
            self.callback(o)
        self.callback(LAST_RECORD)

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by a child node
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            self.status.completed(child_id)
            return
        # Store the results for later...
        self.child_results[child_id].append(record)

    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        """
        raise Exception, "Not implemented"

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(object):
    """
    Abstract Syntax Tree used to represent a Query Plane.
    Acts as a factory.
    """

    def __init__(self, user = None):
        """
        \brief Constructor
        \param user A User instance
        """
        self.user = user
        # The AST is initially empty
        self.root = None

    def get_root(self):
        """
        \return The root Node of this AST (if any), None otherwise
        """
        return self.root

    @returns(bool)
    def is_empty(self):
        """
        \return True iif the AST has no Node.
        """
        return self.get_root() == None


    #@returns(AST)
    def From(self, table, query):
        """
        \brief Build a FROM node
        \param table A Table wrapped by the from operator
        \param query A Query requested to the platform
        \return The updated AST
        """
        assert self.is_empty(),                 "Should be instantiated on an empty AST"
        assert isinstance(table, Table),        "Invalid table = %r (%r)" % (table, type(table))
        assert isinstance(query, Query),        "Invalid query = %r (%r)" % (query, type(query))
        assert len(table.get_platforms()) == 1, "Table = %r should be related to only one platform" % table

        self.query = query
        #platforms = table.get_platforms()
        #platform = list(platforms)[0]

        node = From(table, query) 
        self.root = node
        self.root.callback = self.callback
        return self

    #@returns(AST)
    def union(self, children_ast, key):
        """
        \brief Transforms an AST into a UNION of AST
        \param children_ast A list of AST gathered by this UNION operator
        \param key A Key instance
            \sa manifold.core.key.py 
        \return The AST corresponding to the UNION
        """
        assert isinstance(key, Key),           "Invalid key %r (type %r)"          % (key, type(key))
        assert isinstance(children_ast, list), "Invalid children_ast %r (type %r)" % (children_ast, type(children_ast))

        # If the current AST has already a root node, this node become a child
        # of this Union node ...
        old_root = None
        if not self.is_empty():
            old_root = self.get_root()
            children = [self.get_root()]
        else:
            children = []

        # ... as the other children
        children.extend([ast.get_root() for ast in children_ast])

        self.root = Union(children, key)
        if old_root:
            self.root.callback = old_root.callback
        return self

    #@returns(AST)
    def left_join(self, right_child, predicate):
        """
        \brief Performs a LEFT JOIN between two AST instances
            self ⋈ right_child
        \param right_child An AST instance (right operand of the LEFT JOIN )
        \param predicate A Predicate instance used to perform the join 
        \return The resulting AST
        """
        assert isinstance(right_child, AST),     "Invalid right_child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(Predicate))
        assert not self.is_empty(),              "No left table"

        old_root = self.get_root()
        self.root = LeftJoin(old_root, right_child.get_root(), predicate, None)
        self.root.callback = old_root.callback
        return self

    #@returns(AST)
    def projection(self, fields):
        """
        \brief Performs a SELECT (Projection) on the current AST according to _fields_: 
            ast <- π_fields(ast)
        \param fields the list of fields on which to project
        \return The AST corresponding to the SELECT 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(fields, list), "Invalid fields = %r (%r)" % (fields, type(fields))

        old_root = self.get_root()
        self.root = Projection(old_root, fields)
        self.root.callback = old_root.callback
        return self

    #@returns(AST)
    def selection(self, filters):
        """
        \brief Performs a WHERE (Selection) on the current AST according to _filters_: 
            ast <- σ_filters(ast)
        \param filters A set of Predicate to apply
            TODO We should use here a Clause instance
        \return The AST corresponding to the WHERE 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(filters, set), "Invalid filters = %r (%r)" % (filters, type(filters))
        assert filters != set(),         "Empty set of filters"

        old_root = self.get_root()
        self.root = Selection(old_root, filters)
        self.root.callback = old_root.callback
        return self

    #@returns(AST)
    def subquery(self, children_ast):
        """
        \brief Performs a SUBQUERY operation of the current AST
        \param children_ast the set of children AST to be added as subqueries to
        the current AST
        \return AST corresponding to the SUBQUERY
        """
        assert not self.is_empty(), "AST not initialized"
        old_root = self.get_root()

        self.root = SubQuery(old_root, children_ast)
        self.root.callback = old_root.callback
        return self

    def dump(self, indent = 0):
        """
        \brief Dump the current AST
        \param indent current indentation
        """
        if self.is_empty():
            print "Empty AST (no root)"
        else:
            self.root.dump(indent)

    def start(self):
        """
        \brief Propagates a START message through the AST
        """
        assert not self.is_empty(), "Empty AST, cannot send START message"
        self.get_root().start()

    @property
    def callback(self):
        log_info("I: callback property is deprecated")
        return self.root.callback

    @callback.setter
    def callback(self, callback):
        log_info("I: callback property is deprecated")
        self.root.callback = callback

    def get_callback(self):
        return self.root.callback

    def set_callback(self, callback):
        self.root.set_callback(callback)

#------------------------------------------------------------------
# Example
#------------------------------------------------------------------

def main():
    q = Query("get", "x", [], {}, ["x", "z"], None)

    x = Field(None, "int", "x")
    y = Field(None, "int", "y")
    z = Field(None, "int", "z")

    a = Table(["p"], None, "A", [x, y], [Key([x])])
    b = Table(["p"], None, "B", [y, z], [Key([y])])
    
    ast = AST().From(a, q).left_join(
        AST().From(b, q),
        Predicate(a.get_field("y"), "=", b.get_field("y"))
    ).projection(["x"]).selection(set([Predicate("z", "=", 1)]))

    ast.dump()

if __name__ == "__main__":
    main()

