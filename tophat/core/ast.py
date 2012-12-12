#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import itertools
from tophat.core.filter import Filter, Predicate
from tophat.core.query import Query
from copy import copy, deepcopy
import traceback

# NOTES
# - What about a Record type
# - What about subqueries in records ?

#------------------------------------------------------------------
# Constants used for dumping AST nodes
#------------------------------------------------------------------

DUMPSTR_FROM       = "SELECT %r FROM '%s::%s'" 
DUMPSTR_FROMTABLE  = "SELECT %r FROM [%r, ...]" 
DUMPSTR_PROJECTION = "SELECT [%s]" 
DUMPSTR_SELECTION  = "WHERE %s"
DUMPSTR_UNION      = "UNION"
DUMPSTR_SUBQUERIES = "<subqueries>"

#------------------------------------------------------------------
# Other constants
#------------------------------------------------------------------

# LAST RECORD MARKER
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

class ChildStatus:
    """
    Monitor child completion status
    """

    def __init__(self, all_done_cb):
        """
        \brief Constructor
        \param all_done_cb callback to raise when are children are completed
        """
        self.all_done_cb = all_done_cb
        self.counter = 0

    def started(self, child_id):
        """
        \brief Call this function to signal that a child has completed
        """
        self.counter += child_id + 1

    def completed(self, child_id):
        """
        \brief Call this function to signal that a child has completed
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
        Constructor
        """
        if node:
            if not isinstance(node, Node):
                raise ValueError('Expected type Node, got %s' % node.__class__.__name__)
            return deepcopy(node)
        # Callback triggered when the current node produces data.
        self.callback = None
        # Query representing the data produced by the node.
        self.query = self.query()

    def set_callback(self, callback):
        """
        \brief Associates a callback to the current node
        \param callback The callback to associate
        """

        self.callback = callback
        # Basic information about the data provided by a node

    def query(self):
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
    """

    def __init__(self, query, table, key): # platform, query, config, user_config, user, key):
        """
        \brief Constructor
        \param query
        \param table
        \param key the key for elements returned from the node
        """
        # TODO Depends on the query plane construction
        # Parameters
        self.query, self.table, self.key = query, table, key
        #self.platform, self.query, self.config, self.user_config, self.user, self.key = \
        #        platform, query, config, user_config, user, key
        # Member variables
        # Temporarily store eventual parent subquery records
        self.records = []

        super(From, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """

        # The query returned by a FROM node is exactly the one that was
        # requested
        return self.query

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        q = self.query
        self.tab(indent)
        print DUMPSTR_FROM % (q.fields, self.platform, q.fact_table)

    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
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
        raise Exception, "Cannot call start on a From class, expecting Gateway"

class FromTable(From):
    """
    \brief FROM node querying a list of records
    """

    def __init__(self, query, records=[]):
        """
        \brief Constructor
        """
        self.query, self.records = query, records
        super(FromTable, self).__init__()

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        q = self.query
        self.tab(indent)
        print DUMPSTR_FROMTABLE % (q.fields, self.records[0])

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

#class LeftJoin(Node):
#    """
#    LEFT JOIN operator node
#    """
#
#    def __init__(self, left, right, predicate):
#        """
#        Constructor
#        """
#        # Parameters
#        self.left, self.right, self.predicate = left, right, predicate
#        # Member variables
#        self.left_map = {}
#        # Set up callbacks
#        left.callback, right.callback = self.left_callback, self.right_callback
#
#        super(LeftJoin, self).__init__()
#
#    def query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        lq, rq = self.left.query, self.right.query
#        q = Query(lq)
#        # Filters 
#        # XXX can we join on filtered lists ? I'm not sure !!!
#        # XXX some asserts needed
#        q.filters |= rq.filters
#        # Fields are the union of both sets of fields
#        q.fields  |= rq.fields
#        return q
#        
#    def inject(self, records, only_keys):
#        # XXX Maybe the only_keys argument is optional and can be guessed
#        # TODO Currently we support injection in the left branch only
#        if only_keys:
#            # TODO injection in the right branch
#            # Injection makes sense in the right branch if it has the same key
#            # as the left branch.
#            self.left = self.left.inject(records, only_keys)
#        else:
#            records_inj = []
#            for record in records:
#                proj = do_projection(record, self.left.query.fields)
#                records_inj.append(proj)
#            self.left = self.left.inject(records_inj, only_keys)
#            # TODO injection in the right branch: only the subset of fields
#            # of the right branch
#        return self
#
#    def start(self):
#        """
#        \brief Propagates a START message through the node
#        """
#        node = self.right if self.left_done else self.left
#        node.start()
#
#
#    def dump(self, indent=0):
#        """
#        \brief Dump the current node
#        \param indent current indentation
#        """
#        self.tab(indent)
#        print "JOIN", self.predicate
#        if self.left:
#            self.left.dump(indent+1)
#        else:
#            print '[DATA]', self.left_results
#        self.right.dump(indent+1)
#
#    def left_callback(self, record):
#        """
#        \brief Process records received by the left child
#        \param record dictionary representing the received record 
#        """
#
#        if record == LAST_RECORD:
#            # Inject the keys from left records in the right child...
#            self.right.inject(self.left_map.keys())
#            # ... and start the right node
#            self.right.start()
#            return
#
#        # Directly send records missing information necessary to join
#        if self.predicate not in record or not record[self.predicate]:
#            print "W: Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
#                    (self.predicate, record)
#            self.callback(record)
#
#        # Store the result in a hash for joining later
#        self.left_map[record[self.predicate]] = record
#
#    def right_callback(self, record):
#        """
#        \brief Process records received by the right child
#        \param record dictionary representing the received record 
#        """
#
#        if record == LAST_RECORD:
#            # Send records in left_results that have not been joined...
#            for leftrecord in self.left_results:
#                self.callback(leftrecord)
#            # ... and terminates
#            self.callback(LAST_RECORD)
#            return
#
#        # Skip records missing information necessary to join
#        if self.predicate not in record or not record[self.predicate]:
#            print "W: Missing LEFTJOIN predicate %s in right record %r: ignored" % \
#                    (self.predicate, record)
#            return
#        
#        key = record[self.predicate]
#        # We expect to receive information about keys we asked, and only these,
#        # so we are confident the key exists in the map
#        # XXX Dangers of duplicates ?
#        left_record = self.left_map[key]
#        left_record.update(record)
#        self.callback(left_record)
#
#        del self.left_map[key]


class LeftJoin(Node):
    """
    LEFT JOIN operator node
    """

    def __init__(self, children, joins, callback):
        """
        Constructor
        \param children A list of n Node instances
        \param joins A list of n-1 "join" (= pair of set of mefields)
            joins[i] allows to join child[i] and child[i+1]
        \param callback The callback invoked when the LeftJoin
            returns records. 
        """
        if not isinstance(children, list):
            raise TypeError("Invalid type: %r must be a list" % children)
        if not isinstance(joins, list):
            raise TypeError("Invalid type: %r must be a list" % joins)
        if not children:
            raise ValueError("Invalid parameter: %r is empty" % children)
        if len(joins) != len(children) - 1:
            raise ValueError("Invalid length: %r must have a length of %d" % (joins, len(children) - 1))
        self.children = children
        self.joins = joins
        self.callback = callback
        super(LeftJoin, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        q = Query(self.children[0])
        for child in self.children:
            # XXX can we join on filtered lists ? I'm not sure !!!
            # XXX some asserts needed
            q.filters |= child.filters
            q.fields  |= child.fields
        return q
        
    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
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
            self.left.dump(indent+1)
        else:
            print '[DATA]', self.left_results
        self.right.dump(indent+1)

    def left_callback(self, record):
        """
        \brief Process records received by the left child
        \param record dictionary representing the received record 
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
        \param record dictionary representing the received record 
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

    def __init__(self, node, fields, key):
        """
        Constructor
        \param node
        \param fields
        \param key the key for elements returned from the node
        """
        # Parameters
        self.node, self.fields, self.key = node, fields, key
        # Set up callbacks
        self.node.callback = self.callback

        super(Projection, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        q = Query(self.node.query)
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
        self.node.dump(indent+1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.node.start()

    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
        """
        self.node = self.node.inject(records)
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
# SELECTION node
#------------------------------------------------------------------

class Selection(Node):
    """
    SELECTION operator node (cf WHERE clause in SQL)
    """

    def __init__(self, node, filters, key):
        """
        Constructor
        \param node
        \param filters
        \param key the key for elements returned from the node
        """
        # Parameters
        self.node, self.filters, self.key = node, filters, key
        # Set up callbacks
        self.node.callback = self.callback

        super(Selection, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        q = Query(self.node.query)
        # Selection add filters (union)
        q.filters |= filters
        return q

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print DUMPSTR_SELECTION % self.filters
        self.node.dump(indent+1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.node.start()

    def inject(self, records):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
        """
        self.node = self.node.inject(records)
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD and self.filters:
            record = self.filters.filter(record)
        self.callback(record)

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
            self.status.started(i)

        super(Union, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # We suppose all child queries have the same format, and that we have at
        # least one child
        return Query(self.children[0].query)
        
    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        print DUMPSTR_UNION
        for child in self.children:
            child.dump(indent+1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start all children
        for child in self.children:
            child.start()

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
            self.status.started(i)

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        # Query is unchanged XXX ???
        return Query(self.parent.query)


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
        Modify children queries to take the keys returned by the parent into account
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
        for child in self.children:
            child.start()

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
        record keys
        """
        raise Exception, "Not implemented"

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(object):
    """
    Abstract Syntax Tree used to represent a Query Plane. Acts as a factory.
    """

    def __init__(self, user=None):
        """
        Constructor
        \param router
        \param user
        """
        self.user = user
        # The AST is initially empty
        self.root = None

    def From(self, table, query):
        """
        \brief
        \param table
        \param query query requested to the platform
        """
        # TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! # XXX
        self.query = query
        # XXX print "W: We have two tables providing the same data: CHOICE or UNION ?"
        platforms = table.get_platforms()
        if isinstance(platforms, (list, set, frozenset, tuple)) and len(platforms) > 1:
            children_ast = []
            for p in platforms:
                t = copy(table)
                t.platforms = p
                children_ast.append(AST(self.user).From(t,query))
            self.union(children_ast) # XXX DISJOINT ?
        else:
            platform = list(platforms)[0] if isinstance(platforms, (list, set, frozenset, tuple)) else platforms
            # XXX get rid of router here !
            node = self.router.get_gateway(platform, self.query, self.user)
            node.source_id = self.router.sourcemgr.append(node)
            self.root = node
            self.root.callback = self.callback
        return self

    def union(self, children_ast):
        """
        \brief Transforms an AST into a UNION of AST
        \param children_ast a list of ast to UNION
        \return AST corresponding to the UNION
        """
        # If the current AST has already a root, it becomes the first child
        if self.root:
            old_root = self.root
            children = [self.root]
        else:
            children = []
        # then all all other children
        children.extend(children_ast)

        self.root = Union(children)
        if old_root:
            self.root.callback = old_root.callback
        return self

    # TODO Can we use decorators for such functions ?
    def leftjoin(self, right_child, predicate):
        """
        \brief Performs a LEFT JOIN between the current AST and the _right_
        parameter:
            ast <- ast ⋈ right 
        \param right_child right child of the resulting AST
        \return AST corresponding to the LEFT JOIN
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = LeftJoin(old_root, right_child.root, predicate)
        self.root.callback = old_root.callback
        return self

    def projection(self, fields):
        """
        \brief Performs a PROJECTION on the current AST according to _fields_: 
            ast <- π_fields(ast)
        \param fields the set of fields on which to project
        \return AST corresponding to the PROJECTION
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = Projection(old_root, fields)
        self.root.callback = old_root.callback
        return self

    def selection(self, filters):
        """
        \brief Performs a SELECTION on the current AST according to _filters_: 
            ast <- σ_filters(ast)
        \param filters the set of filters to apply
        \return AST corresponding to the SELECTION
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = Selection(old_root, filters)
        self.root.callback = old_root.callback
        return self

    def subquery(self, children_ast):
        """
        \brief Performs a SUBQUERY operation of the current AST
        \param children_ast the set of children AST to be added as subqueries to
        the current AST
        \return AST corresponding to the SUBQUERY
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = SubQuery(old_root, children_ast)
        self.root.callback = old_root.callback
        return self

    def dump(self, indent=0):
        """
        \brief Dump the current AST
        \param indent current indentation
        """
        if self.root:
            self.root.dump(indent)
        else:
            print "Empty AST (no root)"

    def start(self):
        """
        \brief Propagates a START message through the AST
        """
        self.root.start()

    @property
    def callback(self):
        return self.root.callback

    @callback.setter
    def callback(self, callback):
        self.root.callback = callback
        

def main():
    a = AST().From('A').join(AST().From('B')).projection('c').selection(Eq('c', 'test'))
    a.dump()
#    a.swaphead()
#    a.dump()

if __name__ == "__main__":
    main()

