#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import itertools
from tophat.core.filter import Filter, Predicate
from tophat.core.query import Query
from copy import copy
import traceback

#from tophat.core.nodes import *

# NOTES
# - What about a Record type
# - What about subqueries in records ?

#------------------------------------------------------------------
# Constants used for dumping AST nodes
#------------------------------------------------------------------

DUMPSTR_FROM       = "SELECT %r FROM '%s::%s'" 
DUMPSTR_PROJECTION = "SELECT [%s]" 
DUMPSTR_SELECTION  = "WHERE %s"
DUMPSTR_UNION      = "UNION"
DUMPSTR_SUBQUERIES = "<subqueries>"

#------------------------------------------------------------------
# Other constants
#------------------------------------------------------------------

# LAST RECORD MARKER
LAST_RECORD = None

class Node(object):
    """
    \brief Base class for implementing AST node objects
    """

    def __init__(self):
        """
        Constructor
        """

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

class From(Node):
    """
    \brief FROM node
    """

    def __init__(self, router, platform, query, gateway_config, user_config, user):
        # Parameters
        self.platform, self.query, self.config, self.user_config, self.user = \
                platform, query, gateway_config, user_config, user
        self.router = router

        # Member variables

        # Temporarily store node results
        self.results = []

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

    def inject(self, records, only_keys):
        keys = self.router.metadata_get_keys(self.query.fact_table)
        if not keys:
            raise Exception, "From::inject : key not found for method %s" % self.query.fact_table
        key = next(iter(keys))

        if only_keys:
            # if the requested fields are only the key, no need to perform any
            # query, we can return results immediately
            if self.query.fields == set([key]):
                self.results = [{key: record} for record in records]
                return self
            # otherwise we can request only the key fields
            # XXX make sure filtering is supported sometimes
            self.query.filters.add(Predicate(key, '=', records))
            return self
        else:
            if not records:
                self.results = []
                return self
            # XXX We suppose all records have the same fields
            records_fields = set(records[0].keys())
            if self.query.fields <= records_fields:
                # XXX Do we need to enforce selection/projection ?
                self.results = records
                return self
            # We can make a JOIN between what we have and what we miss
            # do we need to enforce selection/projection here ?
                self.query.fields.difference_update(set(records_fields))
            if not key in self.query.fields:
                self.query.fields.add(key)
            old_self_callback = self.callback
            join = LeftJoin(records, self, key)
            join.callback = old_self_callback
            self.callback = join.right_callback
            join.query = self.query
            return join

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        if self.done:
            for result in self.results:
                self.callback(result)
            self.callback(LAST_RECORD)
            return
        # XXX ??? XXX
        self.do_start()

class LeftJoin(Node):
    """
    LEFT JOIN operator node
    """

    def __init__(self, left, right, predicate):
        """
        Constructor
        """
        # Parameters
        self.left, self.right, self.predicate = left, right, predicate
        # Member variables
        self.left_map = {}
        # Set up callbacks
        left.callback, right.callback = self.left_callback, self.right_callback

        super(LeftJoin, self).__init__()

    def query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        lq, rq = self.left.query, self.right.query
        q = Query(lq)
        # Filters 
        # XXX can we join on filtered lists ? I'm not sure !!!
        # XXX some asserts needed
        q.filters |= rq.filters
        # Fields are the union of both sets of fields
        q.fields  |= rq.fields
        return q
        
    def inject(self, records, only_keys):
        # XXX Maybe the only_keys argument is optional and can be guessed
        # TODO Currently we support injection in the left branch only
        if only_keys:
            # TODO injection in the right branch
            # Injection makes sense in the right branch if it has the same key
            # as the left branch.
            self.left = self.left.inject(records, only_keys)
        else:
            records_inj = []
            for record in records:
                proj = do_projection(record, self.left.query.fields)
                records_inj.append(proj)
            self.left = self.left.inject(records_inj, only_keys)
            # TODO injection in the right branch: only the subset of fields
            # of the right branch
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

class Projection(Node):
    """
    PROJECTION operator node (cf SELECT clause in SQL)
    """

    def __init__(self, node, fields):
        """
        Constructor
        """
        # Parameters
        self.node, self.fields = node, fields
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

    def inject(self, records, only_keys):
        self.node = self.node.inject(records, only_keys)
        return self

    def do_projection(self, record):
        """
        Take the necessary fields in dic
        """
        ret = {}

        # 1/ split subqueries
        local = []
        subqueries = {}
        for f in self.fields:
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


    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD:
            record = do_projection(record)
        self.callback(record)


class Selection(Node):
    """
    SELECTION operator node (cf WHERE clause in SQL)
    """

    def __init__(self, node, filters):
        """
        Constructor
        """
        # Parameters
        self.node, self.filters = node, filters
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

    def inject(self, records, only_keys):
        self.node = self.node.inject(records, only_keys)
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD and self.filters:
            record = self.filters.filter(record)
        self.callback(record)

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
            
class Union(Node):
    """
    UNION operator node
    """

    def __init__(self, router, children):
        """
        Constructor
        """
        # Parameters
        self.router, self.children = router, children
        # Member variables
        #self.child_status = 0
        #self.child_results = {}
        # Stores the list of keys already received to implement DISTINCT
        self.key_list = []
        # Set up callbacks
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)
            self.child_status += i+1

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
        # Compute the key of received records once (will be useful for DISTINCT records)
        # XXX DISTINCT ON (field) could be received as a parameter
        keys = self.router.metadata_get_keys(self.query.fact_table)
        if not keys:
            raise Exception, "Cannot complete query: submethod %s has no key" % self.query.fact_table
        self.key = list(keys).pop()

        # Start all children
        for child in self.children:
            child.start()

    def inject(self, records, only_keys):
        for child in self.children:
            # XXX to change
            child = child.root
            child.inject(records, only_keys)
        return self

    def child_done(self, child_id):
        self.child_status -= child_id + 1
        assert self.child_status >= 0, "Child status error: %d" % self.child_status
        if self.child_status == 0:
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
            self.child_done(child_id)
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

class SubQuery(Node):
    """
    SUBQUERY operator (cf nested SELECT statements in SQL)
    """

    def __init__(self, router, parent, children):
        """
        Constructor
        \param router
        \param parent
        \param children
        """
        # Parameters
        self.parent, self.children = parent, children
        self.router = router
        # Member variables
        self.parent_output = []
        # Set up callbacks
        parent.callback = self.parent_callback
        # Prepare array for storing results from children
        self.child_results = []
        self.child_status = 0
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)
            self.child_results.append([])
            self.child_status += i+1

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
            if self.parent_output: self.run_children()
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

            keys = self.router.metadata_get_keys(child.query.fact_table)
            if not keys:
                raise Exception, "Cannot complete query: submethod %s has no key" % child.query.fact_table
            key = list(keys).pop()
            
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
                        if not key in child_record:
                            # XXX case for links in slice.resource
                            continue
                        if not child_record[key] in child_keys_all:
                            child_record_all.append(child_record)
                            child_keys_all.append(child_record[key])
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
                self.children[i] = child.inject(child_keys_all, only_keys=True)
            else:
                self.children[i] = child.inject(child_record_all, only_keys=False)

        # We make another loop since the children might have been modified in
        # the previous one.
        for child in self.children:
            child.start()

    def child_done(self, child_id):
        self.child_status -= child_id + 1
        assert self.child_status >= 0, "child status error in subquery"
        if self.child_status == 0:
            for o in self.parent_output:
                # Dispatching child results
                for i, child in enumerate(self.children):
                    keys = self.router.metadata_get_keys(child.query.fact_table)
                    if not keys:
                        raise Exception, "From::inject : key not found for method %s" % self.query.fact_table
                    key = next(iter(keys))

                    for record in o[child.query.fact_table]:
                        # Find the corresponding record in child_results and
                        # update the one in the parent with it
                        #child_record = [r for r in self.child_results[i] if key in record and r[key] == record[key]][0]
                        if key in record:
                            for r in self.child_results[i]:
                                if key in r and r[key] == record[key]:
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
            self.child_done(child_id)
            return
        # Store the results for later...
        self.child_results[child_id].append(record)


class AST(object):
    """
    Abstract Syntax Tree used to represent a Query Plane. Acts as a factory.
    """

    def __init__(self, router=None, user=None):
        """
        Constructor
        \param router
        \param user
        """
        self.user = user
        self.router = router

        # The AST is initially empty
        self.root = None

    def from(self, table, query):
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
                children_ast.append(AST(self.router, self.user).From(t,query))
            self.union(children_ast) # XXX DISJOINT ?
        else:
            platform = list(platforms)[0] if isinstance(platforms, (list, set, frozenset, tuple)) else platforms
            node = self.router.get_gateway(platform, self.query, self.user)
            node.source_id = self.router.sourcemgr.append(node)
            self.root = node
            self.root.callback = self.callback
        return self

    def union(self, children_ast):
        # If the current AST has already a root, it becomes the first child
        if self.root:
            old_root = self.root
            children = [self.root]
        else:
            children = []
        # then all all other children
        children.extend(children_ast)

        self.root = Union(self.router, children)
        if old_root:
            self.root.callback = old_root.callback
        return self

    # TODO Can we use decorators for such functions ?
    def leftjoin(self, ast, predicate):
        """
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = LeftJoin(old_root, ast.root, predicate)
        self.root.callback = old_root.callback
        return self

    def projection(self, fields):
        """
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = Projection(old_root, fields)
        self.root.callback = old_root.callback
        return self

    def selection(self, filters):
        """
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = Selection(old_root, filters)
        self.root.callback = old_root.callback
        return self

    def subquery(self, children_ast):
        """
        """
        if not self.root: raise ValueError('AST not initialized')
        old_root = self.root

        self.root = SubQuery(self.router, old_root, children_ast)
        self.root.callback = old_root.callback
        return self

    def dump(self, indent=0):
        """
        \brief Dump the current AST
        \param indent current indentation
        """
        self.root.dump(indent)

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

