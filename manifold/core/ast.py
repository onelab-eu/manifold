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

import sys, random
from copy                       import copy, deepcopy
from types                      import StringTypes

from types                      import StringTypes
from manifold.core.filter       import Filter
from manifold.util.predicate    import Predicate, eq
from manifold.core.query        import Query, AnalyzedQuery
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
DUMPSTR_PROJECTION = "SELECT %s" 
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
        self.counter -= child_id + 1
        assert self.counter >= 0, "Child status error: %d" % self.counter
        if self.counter == 0:
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
        self.identifier = random.randint(0,9999)

    def set_callback(self, callback):
        """
        \brief Associates a callback to the current node
        \param callback The callback to associate
        """
        self.callback = callback
        # Basic information about the data provided by a node

    def get_callback(self):
        """
        \brief Return the callback related to this Node 
        """
        return self.callback

    def send(self, record):
        """
        \brief calls the parent callback with the record passed in parameter
        """
        #print "[%12s #%04d] SEND [ %r ]" % (self.__class__.__name__, self.identifier, record)
        self.callback(record)

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """

        return self.query

        # Raise an exception in the base class to force child classes to
        # implement this method
        #raise Exception, "Nodes should implement the query function: %s" % self.__class__.__name__

    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        print "[%04d]" % self.identifier, ' ' * 4 * indent,
        #        sys.stdout.write(' ' * indent * 4)

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self

    @returns(StringTypes)
    def __repr__(self):
        return "This method should be overloaded!"

    @returns(StringTypes)
    def __str__(self):
        return self.__repr__() 

    def optimize(self):
        tree = self.optimize_selection(Filter())
        #tree = tree.optimize_projection(set())
        return tree
    
    def optimize_selection(self, filter):
        raise Exception, "%s::optimize_selection() not implemented" % self.__class__.__name__

    def optimize_projection(self, fields):
        raise Exception, "%s::optimize_projection() not implemented" % self.__class__.__name__


#------------------------------------------------------------------
# FROM node
#------------------------------------------------------------------

class From(Node):
    """
    \brief FROM node
    From Node are responsible to query a gateway (!= FromTable).
    """

    def __init__(self, platform, query, capabilities):
    #def __init__(self, table, query):
        """
        \brief Constructor
        \param table A Table instance (the 3nf table)
            \sa manifold.core.table.py
        \param query A Query instance: the query passed to the gateway to fetch records 
        """
        assert isinstance(query, Query), "Invalid type: query = %r (%r)" % (query, type(query))
        # XXX replaced by platform name (string)
        #assert isinstance(table, Table), "Invalid type: table = %r (%r)" % (table, type(table))

        #self.query, self.table = query, table
        self.platform, self.query, self.capabilities = platform, query, capabilities
        self.gateway = None
        super(From, self).__init__()

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        print "From::get_query()"
#        # The query returned by a FROM node is exactly the one that was
#        # requested
#        return self.query

    def add_fields_to_query(self, field_names):
        """
        \brief Add field names (list of String) to the SELECT clause of the embedded query
        """
        for field_name in field_names:
            assert isinstance(field_name, StringTypes), "Invalid field_name = %r in field_names = %r" % (field_name, field_names)
        self.query.fields = frozenset(set(self.query.fields) | set(field_names))

#    #@returns(Table)
#    def get_table(self):
#        """
#        \return The Table instance queried by this FROM node.
#        """
#        return self.table

    @returns(StringTypes)
    def get_platform(self):
        """
        \return The name of the platform queried by this FROM node.
        """
        return self.platform
        #return list(self.get_table().get_platforms())[0]

    #@returns(StringTypes)
    def __repr__(self):
        return DUMPSTR_FROM % (
            ', '.join(self.get_query().get_select()),
            self.get_platform(),
            self.get_query().get_from()
        )

    #@returns(From)
    def inject(self, records, key, query):
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
        provided_fields = set(record.keys()) if is_record else set([key])
        needed_fields = self.query.fields
        missing_fields = needed_fields - provided_fields

        if not missing_fields:
            return FromTable(self.query, records, key)


        old_self_callback = self.get_callback()

        # If the inject only provide keys, add a WHERE, else a WHERE+JOIN
        if not is_record or provided_fields < records[0].keys():
            # We will filter the query by inserting a where on 
            list_of_keys = records.keys() if is_record else records
            predicate = Predicate(key, '==', list_of_keys)
            where = Selection(self, Filter().filter_by(predicate))
            where.query = self.query.copy().filter_by(predicate)
            where.set_callback(old_self_callback)
            # XXX need reoptimization
            return where
        else:
            print "From::inject() - INJECTING RECORDS"

        missing_fields.add(key) # |= key
        self.query.fields = missing_fields

        #parent_query = self.query.copy()
        #parent_query.fields = provided_fields
        #parent_from = FromTable(parent_query, records, key)

        join = LeftJoin(records, self, Predicate(key, '==', key))
        join.query = query
        join.set_callback(old_self_callback)

        return join

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        if not self.gateway:
            raise Exception, "Cannot call start on a From class, expecting Gateway"
        self.gateway.start()

    def set_gateway(self, gateway):
        gateway.set_callback(self.get_callback())
        self.gateway = gateway

    def set_callback(self, callback):
        super(From, self).set_callback(callback)
        if self.gateway:
            self.gateway.set_callback(callback)

    def optimize_selection(self, filter):
        if self.capabilities.selection:
            # Push filters into the From node
            for predicate in filter:
                self.query.filters.add(predicate)
            return self
        else:
            # Create a new Selection node
            old_self_callback = self.get_callback()
            selection = Selection(self, filter)
            selection.query = self.query.copy().filter_by(filter)
            selection.set_callback(old_self_callback)
            return selection

class FromTable(From):
    """
    \brief FROM node querying a list of records
    """

    def __init__(self, query, records, key):
        """
        \brief Constructor
        \param query A Query instance
        \param records A list of records (dictionnary instances)
        """
        assert isinstance(query,   Query), "Invalid query = %r (%r)"   % (query,   type(query))
        assert isinstance(records, list),  "Invalid records = %r (%r)" % (records, type(records))

        self.records, self.key = records, key
        super(FromTable, self).__init__(None, query)

    def __repr__(self, indent = 0):
        return DUMPSTR_FROMTABLE % (
            ', '.join(self.get_query().get_select()),
            self.records[0]
        )

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        for record in self.records:
            if not isinstance(record, dict):
                record = {self.key: record}
            self.send(record)
        self.send(LAST_RECORD)

#------------------------------------------------------------------
# LEFT JOIN node
#------------------------------------------------------------------

class LeftJoin(Node):
    """
    LEFT JOIN operator node
    """

    @staticmethod
    def check_init(left_child, right_child, predicate): #, callback):
        #assert issubclass(type(left_child),  Node), "Invalid left child = %r (%r)"  % (left_child,  type(left_child))
        assert issubclass(type(right_child), Node), "Invalid right child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate),    "Invalid predicate = %r (%r)"   % (predicate,   type(predicate))

    def __init__(self, left_child, right_child, predicate):#, callback):
        """
        \brief Constructor
        \param left_child  A Node instance corresponding to left  operand of the LEFT JOIN
        \param right_child A Node instance corresponding to right operand of the LEFT JOIN
        \param predicate A Predicate instance invoked to determine whether two record of
            left_child and right_child can be joined.
        \param callback The callback invoked when the LeftJoin instance returns records. 
        """
        assert predicate.op == eq

        # Check parameters
        LeftJoin.check_init(left_child, right_child, predicate)#, callback)

        # Initialization
        self.left      = left_child
        self.right     = right_child 
        self.predicate = predicate
#        self.set_callback(callback)
        self.left_map  = {}
        if isinstance(left_child, list):
            self.left_done = True
            for r in left_child:
                if isinstance(r, dict):
                    self.left_map[r[self.predicate.key]] = r
                else:
                    self.left_map[r] = {self.predicate.key: r}
        else:
            self.left_done = False
            left_child.set_callback(self.left_callback)
        right_child.set_callback(self.right_callback)

        if not isinstance(left_child, list):
            self.query = self.left.get_query().copy()
            self.query.filters |= self.right.get_query().filters
            self.query.fields  |= self.right.get_query().fields

#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields

        super(LeftJoin, self).__init__()

    @returns(list)
    def get_children(self):
        return [self.left, self.right]

#    @returns(Query)
#    def get_query(self):
#        """
#        \return The query representing AST reprensenting the AST rooted
#            at this node.
#        """
#        print "LeftJoin::get_query()"
#        q = Query(self.get_children()[0])
#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields
#        return q
        
    #@returns(LeftJoin)
    def inject(self, records, key, query):
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
            self.left = self.left.inject(records_inj, key, query) # XXX
            # TODO injection in the right branch: only the subset of fields
            # of the right branch
            return self

        # TODO Currently we support injection in the left branch only
        # Injection makes sense in the right branch if it has the same key
        # as the left branch.
        self.left = self.left.inject(records, key, query) # XXX
        return self

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        node = self.right if self.left_done else self.left
        node.start()

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self
        if isinstance(self.left, list):
            self.tab(indent),
            print '[DATA]', self.left_map.values()
        else:
            self.left.dump(indent + 1)
        self.right.dump(indent + 1)

    def __repr__(self):
        return "JOIN %s %s %s" % self.predicate.get_str_tuple()

    def left_callback(self, record):
        """
        \brief Process records received by the left child
        \param record A dictionary representing the received record 
        """

        if record == LAST_RECORD:
            # left_done. Injection is not the right way to do this.
            # We need to insert a filter on the key in the right member
            predicate = Predicate(self.predicate.value, '==', self.left_map.keys())
            where = Selection(self.right, Filter().filter_by(predicate))
            where.query = self.right.query.copy().filter_by(predicate)
            where.set_callback(self.right.get_callback())
            self.right = where
            self.right = self.right.optimize()
            self.right.set_callback(self.right_callback)
            self.left_done = True
            self.right.start()
            return

            ## Inject the keys from left records in the right child...
            #query = Query().filter_by(self.left.get_query().filters).select(self.predicate.value) # XXX
            #self.right.inject(self.left_map.keys(), self.predicate.value, query)
            ## ... and start the right node
            #return

        # Directly send records missing information necessary to join
        if self.predicate.key not in record or not record[self.predicate.key]:
            print "W: Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                    (self.predicate, record)
            self.send(record)

        # Store the result in a hash for joining later
        self.left_map[record[self.predicate.key]] = record

    def right_callback(self, record):
        """
        \brief Process records received by the right child
        \param record A dictionary representing the received record 
        """
        if record == LAST_RECORD:
            # Send records in left_results that have not been joined...
            for leftrecord in self.left_map.values():
                self.send(leftrecord)
            # ... and terminates
            self.send(LAST_RECORD)
            return

        # Skip records missing information necessary to join
        if self.predicate.value not in record or not record[self.predicate.value]:
            print "W: Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                    (self.predicate, record)
            return
        
        key = record[self.predicate.value]
        # We expect to receive information about keys we asked, and only these,
        # so we are confident the key exists in the map
        # XXX Dangers of duplicates ?
        #print "in Join::right_callback()"
        #self.dump()
        #print "-" * 50
        #print "self.left_map", self.left_map
        #print "searching for key=", key
        left_record = self.left_map[key]
        left_record.update(record)
        self.send(left_record)

        del self.left_map[key]

    def optimize_selection(self, filter):
        # LEFT JOIN
        # We are pushing selections down as much as possible:
        # - selection on filters on the left: can push down in the left child
        # - selection on filters on the right: cannot push down
        # - selection on filters on the key / common fields ??? TODO
        parent_filter, left_filter = Filter(), Filter()
        for predicate in filter:
            if predicate.key in self.left.get_query().fields:
                left_filter.add(predicate)
            else:
                parent_filter.add(predicate)

        if left_filter:
            print "LEFT FILTER", left_filter
            self.left = self.left.optimize_selection(left_filter)
            #selection = Selection(self.left, left_filter)
            #selection.query = self.left.copy().filter_by(left_filter)
            self.left.set_callback(self.left_callback)
            #self.left = selection
        else:
            print "NO LEFT FILTER"

        if parent_filter:
            print "PARENT_FILTER"
            old_self_callback = self.get_callback()
            selection = Selection(self, parent_filter)
            selection.query = self.query.copy().filter_by(parent_filter)
            selection.set_callback(old_self_callback)
            return selection
        else:
            print "NO PARENT FILTER"
        return self

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
        \param fields A list of Field instances corresponding to
            the fields we're selecting.
        """
        for field in fields:
            assert isinstance(field, Field), "Invalid field %r (%r)" % (field, type(field))
        self.child, self.fields = child, fields
        self.child.set_callback(self.get_callback())

        self.query = self.child.get_query().copy()
        self.query.fields &= fields

        super(Projection, self).__init__()

    @returns(list)
    def get_fields(self):
        """
        \returns The list of Field instances selected in this node.
        """
        return self.fields

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return The Query representing the data produced by the nodes.
#        """
#        print "Projection()::get_query()"
#        q = Query(self.child.get_query())
#
#        # Projection restricts the set of available fields (intersection)
#        q.fields &= fields
#        return q

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self 
        self.child.dump(indent+1)

    def __repr__(self):
        return DUMPSTR_PROJECTION % ", ".join([field.get_name() for field in self.get_fields()])

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()

    #@returns(Projection)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records A list of dictionaries representing records,
                       or a list of record keys
        \return This node
        """
        self.child = self.child.inject(records, key, query) # XXX
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD:
            record = do_projection(record)
        self.send(record)

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
        self.child.set_callback(self.get_callback())

        self.query = self.child.get_query().copy()
        self.query.filters |= filters

        super(Selection, self).__init__()

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the childs.
#        \return query representing the data produced by the childs.
#        """
#        print "Selection::get_query()"
#        q = Query(self.child.get_query())
#
#        # Selection add filters (union)
#        q.filters |= filters
#        return q

    def dump(self, indent = 0):
        """
        \brief Dump the current child
        \param indent The current indentation
        """
        self.tab(indent)
        print "%r" % self
        self.child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_SELECTION % ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self.filters])

    def start(self):
        """
        \brief Propagates a START message through the child
        """
        self.child.start()

    #@returns(Selection)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the child
        \param records A list of dictionaries representing records,
                       or list of record keys
        \return This node
        """
        self.child = self.child.inject(records, key, query) # XXX
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node 
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD or (self.filters and self.filters.match(record)):
            self.send(record)
        #if record != LAST_RECORD and self.filters:
        #    record = self.filters.filter(record)
        #self.send(record)

    def optimize_selection(self, filter):
        # Concatenate both selections...
        for predicate in self.filters:
            filter.add(predicate)
        return self.child.optimize_selection(filter)

#------------------------------------------------------------------
# DEMUX node
#------------------------------------------------------------------

class Demux(Node):

    def __init__(self, child):
        """
        \brief Constructor
        \param child A Node instance, child of this Dup Node
        """
        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        super(Demux, self).__init__()

    def add_callback(self, callback):
        """
        \brief Add a parent callback to this Node
        """
        self.parent_callbacks.append(callback)

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        for callback in self.parent_callbacks:
            callbacks(record)

    @returns(StringTypes)
    def __repr__(self):
        return "DEMUX (built above %r)" % self.get_child() 

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self
        self.get_child().dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()
        self.status.started(0)

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def add_parent(self, parent):
        """
        \brief Add a parent Node to this Demux Node.
        \param parent A Node instance
        """
        assert issubclass(Node, type(parent)), "Invalid parent %r (%r)" % (parent, type(parent))
        print "not yet implemented"

#------------------------------------------------------------------
# DUP node
#------------------------------------------------------------------

class Dup(Node):

    def __init__(self, child, key):
        """
        \brief Constructor
        \param child A Node instance, child of this Dup Node
        \param key A Key instance
        """
        #assert issubclass(Node, type(child)), "Invalid child %r (%r)" % (child, type(child))
        #assert isinstance(Key,  type(key)),   "Invalid key %r (%r)"   % (key,   type(key))

        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        self.child_results = set()
        super(Dup, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        return Query(self.child.get_query())

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "DUP (built above %r)" % self.get_child()
        self.get_child().dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()
        self.status.started(0)

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by a child node
        \param record dictionary representing the received record
        """
        assert child_id == 0

        if record == LAST_RECORD:
            self.status.completed(child_id)
            return

        if record not in self.child_results:
            self.child_results.add(record)
            self.send(record)
            return


#------------------------------------------------------------------
# UNION node
#------------------------------------------------------------------
            
class Union(Node):
    """
    UNION operator node
    """

    def __init__(self, children, key):
        """
        \brief Constructor
        \param children A list of Node instances, the children of
            this Union Node.
        \param key A Key instance, corresponding to the key for
            elements returned from the node
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
            child.set_callback(ChildCallback(self, i))

        # We suppose all children have the same format...
        self.query = self.children[0].get_query()

        super(Union, self).__init__()

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        # We suppose all child queries have the same format, and that we have at
#        # least one child
#        print "Union::get_query()"
#        return Query(self.children[0].get_query())
        
    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self
        for child in self.children:
            child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_UNION

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start all children
        for i, child in enumerate(self.children):
            self.status.started(i)
        for i, child in enumerate(self.children):
            child.start()

    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
        """
        for i, child in enumerate(self.children):
            self.children[i] = child.inject(records, key, query)
        return self

    def all_done(self):
        #for record in self.child_results.values():
        #    self.send(record)
        self.send(LAST_RECORD)

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by the child node
        \param child_id identifier of the child that received the record
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            self.status.completed(child_id)
            return
        
        key = self.key.get_name()

        # DISTINCT not implemented, just forward the record
        if not key:
            self.send(record)
            return
        # Ignore records that have no key
        if key not in record:
            print "W: UNION ignored record without key"
            return
        # Ignore duplicate records
        #if record[key] in key_list:
        #    print "W: UNION ignored duplicate record"
        #    return
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
            #selection = Selection(child, filter)
            #selection.query = child.query.copy().filter_by(filter)
            self.children[i].set_callback(old_child_callback)
            #selection.set_callback(child.get_callback())
            #self.children[i] = selection
        return self

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
        parent.set_callback(self.parent_callback)

        # Prepare array for storing results from children: parent result can
        # only be propagated once all children have replied
        self.child_results = []
        self.status = ChildStatus(self.all_done)
        for i, child in enumerate(self.children):
            child.set_callback(ChildCallback(self, i))
            self.child_results.append([])

        super(SubQuery, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # Query is unchanged XXX ???
        return Query(self.parent.get_query())

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.parent.dump(indent + 1)
        if not self.children: return
        self.tab(indent)
        print "%r" % self 
        for child in self.children:
            child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_SUBQUERIES

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start the parent first
        self.parent.start()

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
            # We have two cases:
            # (1) either the parent query has subquery fields (a list of child
            #     ids + eventually some additional information)
            # (2) either the child has a backreference to the parent
            #
            # In all cases, we will collect all identifiers to proceed to a
            # single child query for efficiency purposes, unless it's not
            # possible (?).
            #
            # We have several parent records stored in self.parent_output
            #
            # /!\ Can we have a mix of (1) and (2) ? For now, let's suppose NO.
            #  *  We could expect key information to be stored in the DBGraph

            parent_query = self.parent.get_query()
            child_query  = child.get_query()

            #print "PARENT_TABLE", parent_query.fact_table
            parent_fields = self.key
            #print "----"
            print "PARENT FIELDS", parent_fields
            print "CHILD FIELDS", child_query.fields
            #print "----"

            # (1) in the parent, we might have a field named after the child
            # method containing either records or identifiers of the children
            
            if child_query.fact_table in parent_query.fields:
                pass

            elif parent_fields <= child_query.fields:
                # CASE (2) : The child has a backreference to the parent
                # We will inject a where condition in the child
                # /!\ Complicated when the parent has a multi_key
                # Collect parent ids
                
                # XXX
                parent_ids = [record[parent_key] for record in self.parent_output]

                # Inject a where in the right child (NOTE: we could have planned this where since the beginning maybe ?)
                predicate = Predicate(parent_key, '==', parent_ids)
                old_child_callback= child.get_callback()
                where = Selection(child, Filter().filter_by(predicate))
                where.query = child.query.copy().filter_by(predicate)
                where.set_callback(child.get_callback())
                #self.children[i] = where
                self.children[i] = where.optimize()
                self.children[i].set_callback(old_child_callback)
            else:
                raise Exception, "No link between parent and child queries"

            #print "-" * 50
            #print "UPDATED CHILDREN"
            #print "-" * 50
            #self.dump()
            #print "-" * 50

            # The parent_output should already have either a set of keys or a
            # set of records. They will serve an input for the JOINS
            # keys = filters for the left part of the first JOIN
            # records = (partial) replacement for the first JOIN
            #     we can request less and make a JOIN with existing data

            # Collect the list of all child records in order to make a single
            # child query; we will have to dispatch the results in all_done
            # later on.

#DEPRECATED#            child_record_all = []
#DEPRECATED#            child_keys_all = []
#DEPRECATED#            only_key = False # We have only the key in child records
#DEPRECATED#            for parent_record in self.parent_output:
#DEPRECATED#                # XXX We are supposing 1..N here
#DEPRECATED#                if not isinstance(parent_record[child.query.fact_table], list):
#DEPRECATED#                    raise Exception, "run_children received 1..1 record; 1..N implemented only."
#DEPRECATED#                for child_record in parent_record[child.query.fact_table]:
#DEPRECATED#                    if isinstance(child_record, dict):
#DEPRECATED#                        if not self.key in child_record:
#DEPRECATED#                            # XXX case for links in slice.resource
#DEPRECATED#                            continue
#DEPRECATED#                        if not child_record[self.key] in child_keys_all:
#DEPRECATED#                            child_record_all.append(child_record)
#DEPRECATED#                            child_keys_all.append(child_record[self.key])
#DEPRECATED#                        # else duplicate
#DEPRECATED#                    else:
#DEPRECATED#                        # XXX We should be injecting keys only what is common
#DEPRECATED#                        # between all records. For this we should be remembering
#DEPRECATED#                        # the list of child_record fields that are in common in
#DEPRECATED#                        # all child records.
#DEPRECATED#                        only_key = True
#DEPRECATED#                        child_keys_all.append(child_record)
#DEPRECATED#            
#DEPRECATED#            # child is the AST receiving the injection
#DEPRECATED#            if only_key:
#DEPRECATED#                self.children[i] = child.inject(child_keys_all, key) # XXX key ?
#DEPRECATED#            else:
#DEPRECATED#                self.children[i] = child.inject(child_record_all, key) # XXX key ?

        # We make another loop since the children might have been modified in
        # the previous one.
        for i, child in enumerate(self.children):
            self.status.started(i)
        for i, child in enumerate(self.children):
            child.start()

    def all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """
        print "SubQuery::all_done()"
        # XXX Suppose simple keys
        key = self.key.get_name()

        for o in self.parent_output:
            print " > parent output", o
            print "-----------------"
            # Dispatching child results
            for i, child in enumerate(self.children):
                if not child.query.fact_table in o:
                    # (2)
                    o[child.query.fact_table] = []
                    for child_record in self.child_results[i]:
                        if child_record[key] == o[key]:
                            o[child.query.fact_table].append(child_record)
                else:
                    # (1)
                    for record in o[child.query.fact_table]:
                        # Find the corresponding record in child_results and
                        # update the one in the parent with it
                        if key in record:
                            for r in self.child_results[i]:
                                if key in r and r[key] == record[key]:
                                    record.update(r)
                        # XXX We ignore missing keys
            self.send(o)
        self.send(LAST_RECORD)

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

    def inject(self, records, key, query):
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
    def From(self, platform, query, capabilities):
    #def From(self, table, query):
        """
        \brief Append a FROM Node to this AST
        \param table The Table wrapped by the FROM operator
        \param query The Query sent to the platform
        \return The updated AST
        """
        assert self.is_empty(),                 "Should be instantiated on an empty AST"
        #assert isinstance(table, Table),        "Invalid table = %r (%r)" % (table, type(table))
        assert isinstance(query, Query),        "Invalid query = %r (%r)" % (query, type(query))
        #assert len(table.get_platforms()) == 1, "Table = %r should be related to only one platform" % table

        # USELESS ? # self.query = query
#OBSOLETE|        platforms = table.get_platforms()
#OBSOLETE|        platform = list(platforms)[0]

        node = From(platform, query, capabilities) 
        self.root = node
        self.root.set_callback(self.get_callback())
        return self

    #@returns(AST)
    def union(self, children_ast, key):
        """
        \brief Make an AST which is the UNION of self (left operand) and children_ast (right operand)
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

        if len(children) > 1:
            self.root = Union(children, key)
        else:
            self.root = children[0]
        if old_root:
            self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def left_join(self, right_child, predicate):
        """
        \brief Make an AST which is the LEFT JOIN of self (left operand) and children_ast (right operand) 
            self ⋈ right_child
        \param right_child An AST instance (right operand of the LEFT JOIN )
        \param predicate A Predicate instance used to perform the join 
        \return The resulting AST
        """
        assert isinstance(right_child, AST),     "Invalid right_child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(Predicate))
        assert not self.is_empty(),              "No left table"

        old_root = self.get_root()
        self.root = LeftJoin(old_root, right_child.get_root(), predicate)#, None)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def demux(self):
        """
        \brief Append a DEMUX Node above this AST
        \return The updated AST 
        """
        assert not self.is_empty(),      "AST not initialized"

        old_root = self.get_root()
        self.root = Demux(old_root)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def dup(self, key):
        """
        \brief Append a DUP Node above this AST
        \param key A Key instance, allowing to detecting duplicates
        \return The updated AST 
        """
        assert not self.is_empty(),      "AST not initialized"

        old_root = self.get_root()
        self.root = Dup(old_root, key)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def projection(self, fields):
        """
        \brief Append a SELECT Node (Projection) above this AST
            ast <- π_fields(ast)
        \param fields the list of fields on which to project
        \return The AST corresponding to the SELECT 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(fields, list), "Invalid fields = %r (%r)" % (fields, type(fields))

        old_root = self.get_root()
        self.root = Projection(old_root, fields)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def selection(self, filters):
        """
        \brief Append a WHERE Node (Selection) above this AST
            ast <- σ_filters(ast)
        \param filters A set of Predicate to apply
        \return The AST corresponding to the WHERE 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(filters, set), "Invalid filters = %r (%r)" % (filters, type(filters))
        assert filters != set(),         "Empty set of filters"

        old_root = self.get_root()
        self.root = Selection(old_root, filters)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def subquery(self, children_ast, parent_key):
        """
        \brief Append a SUBQUERY Node above the current AST
        \param children_ast the set of children AST to be added as subqueries to
            the current AST
        \return AST corresponding to the SUBQUERY
        """
        assert not self.is_empty(), "AST not initialized"
        old_root = self.get_root()

        self.root = SubQuery(old_root, children_ast, parent_key)
        self.root.set_callback(old_root.get_callback())
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
        return self.root.get_callback()

    def set_callback(self, callback):
        self.root.set_callback(callback)

    def optimize(self):
        self.root.optimize()

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
