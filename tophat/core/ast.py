#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
from tophat.core.filter import Filter, Predicate
from tophat.core.query import Query
from copy import copy
import traceback

#from tophat.core.nodes import *

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

class Node(object):

    def set_callback(self, callback):
        self.callback = callback
        # Basic information about the data provided by a node

class FromNode(Node):

    def __init__(self, router, platform, query, gateway_config, user_config, user):
        self.platform = platform
        self.query = query
        self.gateway_config = gateway_config
        self.user_config = user_config
        self.started = False
        self.callback = None
        self.router = router
        self.done = False
        self.results = []
        self.dup = False
        self.user = user

    def dump(self, indent=0):
        print ' ' * indent * 4, "SELECT %r FROM '%s:%s'" % (self.query.fields, self.platform, self.query.fact_table)

    def inject(self, records, only_keys):
        keys = self.router.metadata_get_keys(self.query.fact_table)
        if not keys:
            raise Exception, "FromNode::inject : key not found for method %s" % self.query.fact_table
        key = next(iter(keys))

        if only_keys:
            # if the requested fields are only the key, no need to perform any
            # query, we can return results immediately
            if self.query.fields == set([key]):
                self.results = [{key: record} for record in records]
                self.done = True
                return self
            # otherwise we can request only the key fields
            # XXX make sure filtering is supported sometimes
            self.query.filters.add(Predicate(key, '=', records))
            return self
        else:
            if not records:
                self.results = []
                self.done = True
                return self
            # XXX We suppose all records have the same fields
            records_fields = set(records[0].keys())
            if self.query.fields <= records_fields:
                # XXX Do we need to enforce selection/projection ?
                self.results = records
                self.done = True
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
            return join

#    def install(self, router, callback, start):
#        self.router = router
#        node = router.get_gateway(self.table.platform, self.table.name, self.fields)
#        node.do_start = start
#        self._source_id = router.sourcemgr.append(node, start=start)
#        return node

    def start(self):
        if self.done:
            for result in self.results:
                self.callback(result)
            if self.dup:
                raise Exception, "dup"
            self.dup = True
            self.callback(None)
            return
        self.started = True
        self.do_start()

class LeftJoin(Node):

    def __init__(self, left, right, predicate):
        self.callback = None
        self.query = None
        # Parameters
        self.right = right
        self.predicate = predicate
        # Set up callbacks
        if isinstance(left, list):
            self.left = None
            self.left_table = left
            self.left_done = True
        else:
            self.left = left
            self.left_table = []
            self.left_done = False
            left.callback = self.left_callback
        right.callback = self.right_callback
        # Local variables
        self.right_map = {}
        self.right_done = False

    def start(self):
        if self.left_done:
            self.right.start()
        else:
            self.left.start()

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

    def dump(self, indent=0):
        print ' ' * indent * 4, "JOIN", self.predicate
        if self.left:
            self.left.dump(indent+1)
        else:
            print '[DATA]', self.left_table
        self.right.dump(indent+1)

    def left_callback(self, record):
        if not record:
            if self.left_done:
                raise Exception, "pouet"
            self.left_done = True
            self.right.start()
            return
        # New result from the left operator
        if self.right_done:
            if self.left_table:
                self.process_left_table()
                self.left_table = []
            self.process_left_record(record)
        else:
            self.left_table.append(record)

#traceback.print_exc()

    def process_left_record(self, record):
        # We cannot join because the left has no key
        if self.predicate not in record:
            self.callback(record)
            return
        if record[self.predicate] in self.right_map:
            record.update(self.right_map[record[self.predicate]])
            del self.right_map[record[self.predicate]]
        self.callback(record)
        # Handling remaining values from JOIN
        # XXX This is not a left join !!
        #for r in self.right_map.values():
        #    yield r
        #print "left[%s] = %d, right[%s] = %d" % (self.left._node, cptl, self.right._node, cptr)

    def process_left_table(self):
        for record in self.left_table:
            self.process_left_record(record)

    def right_callback(self, record):
        try:
            #print "right callback", record
            # We need to send a NULL record to signal the end of the table
            if not record:
                self.right_done = True
                if self.left_done:
                    self.process_left_table()
                    self.callback(None)
                return
            # New result from the right operator
            #
            # Let's build a map according to predicate = simple field name to begin with
            if self.predicate not in record or not record[self.predicate]:
                # We skip records with missing join information
                return
            self.right_map[record[self.predicate]] = record
        except Exception, e:
            print "Exception during LeftJoin::right_callback(%r): %s" % (record, e)
            traceback.print_exc()

class Projection(Node):

    def __init__(self, node, fields):
        self.callback = None
        self.query = None
        self.node = node
        self.fields = fields
        # Set up callbacks
        self.node.callback = self.callback

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT [%s]" % self.fields
        self.node.dump(indent+1)

    def start(self):
        self.node.start()

    def inject(self, records, only_keys):
        self.node = self.node.inject(records, only_keys)
        return self

    def callback(self, record):
        try:
            if not record:
                self.callback(record)
                return
            ret = do_projection(record, self.fields)
            self.callback(ret)

        except Exception, e:
            print "Exception during ProjectionNode::callback(%r): %s" % (record, e)
            traceback.print_exc()


class Selection(Node):

    def __init__(self, node, filters):
        self.callback = None
        self.query = None
        self.node = node
        self.filters = filters

    def dump(self, indent=0):
        print ' ' * indent * 4, "WHERE %s"  % self.filters
        self.node.dump(indent+1)

    def start(self):
        self.node.start()

    def inject(self, records, only_keys):
        self.node = self.node.inject(records, only_keys)
        return self

    def callback(self, record):
        try:
            if not record:
                self.callback(record)
                return 
            if self.filters:
                record = self.filters.filter(record)
            self.callback(record)
        except Exception, e:
            print "Exception during SelectionNode::callback: %s" % e
            traceback.print_exc()

    def inject(self, records):
        # XXX improve here
        self.node.inject(records)
        return self

class ChildCallback:
    def __init__(self, parent, child_id):
        self.parent = parent
        self.child_id = child_id

    def __call__(self, record):
        self.parent.child_callback(self.child_id, record)
            
class Union(Node):

    def __init__(self, router, children):
        self.callback = None
        self.query = None
        self.router = router
        self.children = children
        self.child_status = 0
        self.child_results = {}
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)
            self.child_status += i+1

    def dump(self, indent=0):
        print ' ' * indent * 4, 'UNION'
        for child in self.children:
            child.dump(indent+1)

    def start(self):
        # Compute key
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
            self.callback(None)

    def child_callback(self, child_id, record):
        #if not record:
        #    self.parent.child_done(self.child_id)
        #    return
        #self.parent.callback(record)
        if not record:
            self.child_done(child_id)
            return
        # Merge results...
        if not self.key in record:
            print "W: ignored record without key"
            return
        if record[self.key] in self.child_results:
            # Merge ! Fields must be the same, subfield sets are joined
            previous = self.child_results[record[self.key]]
            for k,v in record.items():
                if not k in previous:
                    previous[k] = v
                    continue
                if isinstance(v, list):
                    previous[k].extend(v)
                else:
                    if not v == previous[k]:
                        print "W: ignored conflictual field"
                    # else: nothing to do
        else:
            self.child_results[record[self.key]] = record

class SubQuery(Node):

    def __init__(self, router, parent, children):
        self.callback = None
        self.query = None
        # for AST
        self.router = router
        self.parent = parent
        self.children = children
        # Local storage
        self.parent_output = []
        # Set up callbacks
        parent.callback = self.parent_callback
        self.child_status = 0
        # Prepare array for storing results from children
        self.child_results = []
        for i, child in enumerate(self.children):
            child.callback = ChildCallback(self, i)
            self.child_status += i+1
            self.child_results.append([])

    def dump(self, indent=0):
        self.parent.dump(indent+1)
        if not self.children: return
        print ' ' * (indent+1) * 4, "<subqueries>"
        for child in self.children:
            child.dump(indent+1)

    def start(self):
        self.parent.start()

    def parent_callback(self, record):
        if record:
            self.parent_output.append(record)
            return
        # When we have received all parent records, we can run children
        if self.parent_output:
            self.run_children()

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

#            # old #####"
#            ast = child.root
#            # We suppose all results have the same shape, and that we have at
#            # least one result
#            if ast.query.fact_table not in self.parent_output[0]:
#                raise Exception, "Missing primary key information in parent for child %s" % ast.query.fact_table
#
#            keys = self.router.metadata_get_keys(ast.query.fact_table)
#            if not keys:
#                raise Exception, "Cannot complete query: submethod %s has no key" % method
#            key = list(keys).pop()
#
#            # Create a JOiN node
#
#            fact_table = ast.query.fact_table
#            filters = ast.query.filters.union(ast.query.filters)
#            fields = ast.query.fields.union(ast.query.fields)
#
#            fact_table_key = self.router.metadata_get_keys(fact_table)
#            fact_table_key = next(iter(fact_table_key))
#
#            parent_keys = set([])
#            for record in self.parent_output: 
#                # eg. record == slice
#                #     record[fact_table] == resource[]
#                #       this variable hold the set of information we already
#                #       have for each resource in the slice !
#                #       -> we only need to request the rest, alongside with the
#                #       key... note that all fields might be useful to complete
#                #       the query, and not only the key !
#                print "--------"
#                print set([ x[fact_table_key] for x in record[fact_table] if fact_table_key in x])
#                parent_keys.update(set([ x[fact_table_key] for x in record[fact_table] if fact_table_key in x]))
#            print "======="
#            print parent_keys
#            import sys
#            sys.exit(0)
#            join = LeftJoin([x[fact_table] for x in self.parent_output], child, key)
#
#            # Update its query
#            join.query = Query(fact_table=fact_table, filters=filters, params=params, fields=fields)
#
#            join.callback = lambda record: self.child_callback(i, record)
#
#            # This JOiN node becomes the new child
#            self.children[i] = join
#
#            # Can itself be a subquery
#            # Collect keys from parent results
#            parent_keys = []
#            for o in self.parent_output:
#                if self.parent.query.fact_table in o:
#                    # o[method] can be :
#                    # - an id (1..)
#                    # - an object (1..1)
#                    # - a list of id (1..N)
#                    # - a list of objects (1..N)
#                    if isinstance(o[ast.query.fact_table], list):
#                        # We inspected each returned object or key
#                        for x in o[ast.query.fact_table]:
#                            if isinstance(x, dict):
#                                # - get key from metadata and add it
#                                # - What to do with remaining fields, we
#                                #   might not be able to get them
#                                #   somewhere else
#                                raise Exception, "returning subobjects not implemented (yet)."
#                            else:
#                                parent_keys.append(x)
#                    else:
#                        # 1..1
#                        raise Exception, "1..1 relationships are not implemented (yet)."
#                    parent_keys.extend(o['key']) # 1..N
#
#            # Add filter on method key
#            if ast.query.filters.has_key(key):
#                raise Exception, "Filters on keys are not allowed (yet) for subqueries"
#            # XXX careful frozen sets !!
#            ast.query.filters.add(Predicate(key, '=', parent_keys))
#
#        # Run child nodes
#        #print "I: running subquery children"
#        for child in self.children:
#            child.start()

    def child_done(self, child_id):
        self.child_status -= child_id + 1
        assert self.child_status >= 0, "child status error in subquery"
        if self.child_status == 0:
            for o in self.parent_output:
                # Dispatching child results
                for i, child in enumerate(self.children):
                    keys = self.router.metadata_get_keys(child.query.fact_table)
                    if not keys:
                        raise Exception, "FromNode::inject : key not found for method %s" % self.query.fact_table
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
            self.callback(None)

    def child_callback(self, child_id, record):
        if not record:
            self.child_done(child_id)
            return
        self.child_results[child_id].append(record)

# in Filter ?
def match_filters(dic, filter):
    # We suppose if a field is in filter, it is therefore in the dic
    if not filter:
        return True
    match = True
    for k, op, v in filter:
        if k not in dic:
            return False

        if op == '=':
            if isinstance(v, list):
                match &= (dic[k] in v) # array ?
            else:
                match &= (dic[k] == v)
        elif op == '~':
            if isinstance(v, list):
                match &= (dic[k] not in v) # array ?
            else:
                match &= (dic[k] != v) # array ?
        elif op == '<':
            if isinstance(v, StringTypes):
                # prefix match
                match &= dic[k].startswith('%s.' % v)
            else:
                match &= (dic[k] < v)
        elif op == '[':
            if isinstance(v, StringTypes):
                match &= dic[k] == v or dic[k].startswith('%s.' % v)
            else:
                match &= (dic[k] <= v)
        elif op == '>':
            if isinstance(v, StringTypes):
                # prefix match
                match &= v.startswith('%s.' % dic[k])
            else:
                match &= (dic[k] > v)
        elif op == ']':
            if isinstance(v, StringTypes):
                # prefix match
                match &= dic[k] == v or v.startswith('%s.' % dic[k])
            else:
                match &= (dic[k] >= v)
        elif op == '&':
            match &= (dic[k] & v) # array ?
        elif op == '|':
            match &= (dic[k] | v) # array ?
        elif op == '{':
            match &= (v in dic[k])
        if not match:
            return False
    return match
                



class Filter(object):
    def __init__(self, op, field, value):
        self._op = op
        self._field = field
        self._value = value

class Eq(Filter): 
    def __init__(self, field, value):
        super(Eq, self).__init__('==', field, value)

    def dump(self):
        return "%s %s %s" % (self._field, self._op, self._value)


class AST(object):
    def __init__(self, router=None, user=None):
        # Empty request
        self.root = None
        self.router = router
        self.user = user

    def _get(self):
        return self.root._get()
        
    def get(self):
        return list(self._get())

    def From(self, table, query):
        """
        """
        self.query = query
        # XXX print "W: We have two tables providing the same data: CHOICE or UNION ?"
        if isinstance(table.platform, list) and len(table.platform)>1:
            children_ast = []
            for p in table.platform:
                t = copy(table)
                t.platform = p
                children_ast.append(AST(self.router, self.user).From(t,query))
            self.union(children_ast) # XXX DISJOINT ?
        else:
            node = self.router.get_gateway(table.platform, self.query, self.user)
            node.source_id = self.router.sourcemgr.append(node)
            self.root = node
            self.root.callback = self.callback
        return self

    def union(self, children_ast):
        children = [self.root] if self.root else []
        children.extend(children_ast)
        q = children[0].query

        self.root = Union(self.router, children)
        self.root.query = Query(action=q.action, fact_table=q.fact_table, filters=q.filters, fields=q.fields)
        return self

    # TODO Can we use decorators for such functions ?
    def join(self, ast, predicate):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = LeftJoin(old_root, ast.root, predicate)

        action = old_root.query.action
        params = old_root.query.params
        fact_table = old_root.query.fact_table
        filters = old_root.query.filters.union(ast.root.query.filters)
        fields = old_root.query.fields.union(ast.root.query.fields)
        self.root.query = Query(action=action, fact_table=fact_table, filters=filters, params=params, fields=fields)

        self.root.callback = old_root.callback

        return self

    def projection(self, fields):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = Projection(old_root, fields)

        action = old_root.query.action
        params = old_root.query.params
        fact_table = old_root.query.fact_table
        filters = old_root.query.filters
        fields = fields
        self.root.query = Query(action=action, fact_table=fact_table, filters=filters, params=params, fields=fields)

        self.root.callback = old_root.callback

        return self

    def selection(self, filters):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = Selection(old_root, filters)

        action = old_root.query.action
        params = old_root.query.params
        fact_table = old_root.query.fact_table
        filters = old_root.query.filters.union(filters)
        fields = old_root.query.fields
        self.root.query = Query(action=action, fact_table=fact_table, filters=filters, params=params, fields=fields)

        self.root.callback = old_root.callback

        return self

    def subquery(self, children_ast):
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = SubQuery(self.router, old_root, children_ast)

        action = old_root.query.action
        params = old_root.query.params
        fact_table = old_root.query.fact_table
        filters = old_root.query.filters
        fields = old_root.query.fields
        self.root.query = Query(action=action, fact_table=fact_table, filters=filters, params=params, fields=fields)

        self.root.callback = old_root.callback

        return self

    def dump(self, indent=0):
        self.root.dump(indent)

    def start(self):
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

