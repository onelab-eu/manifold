#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
from tophat.core.filter import Filter, Predicate
from tophat.core.query import Query

#from tophat.core.nodes import *

class Node(object):

    def set_callback(self, callback):
        self.callback = callback
        # Basic information about the data provided by a node

class FromNode(Node):

    def __init__(self, router, platform, query, config):
        self.platform = platform
        self.query = query
        self.config = config
        self.do_start = False
        self.started = False
        self.callback = None
        self.router = router

    def dump(self, indent=0):
        print ' ' * indent * 4, "SELECT %r FROM '%s'" % (self.query.fields, self.query.fact_table)

    def inject(self, records):
        print "Injecting records:", records
        injected_fields = [field for field in records[0].keys()]
        for f in injected_fields:
            if f in self.router.metadata_get_keys(self.query.fact_table):
                # We have primary keys : inject filter 
                # if already present: Exception
                # XXX This code is already written
                pass
            else:
                # inject a JOIN for which we already have the results
                # can do it manually
                # self.need_merge_with_injection = True
                # XXX
                pass
        self.node.inject(records)

    def install(self, router, callback, start):
        self.router = router
        node = router.get_gateway(self.table.platform, self.table.name, self.fields)
        node.do_start = start
        self._source_id = router.sourcemgr.append(node, start=start)
        return node

    def start(self):
        if self.router and self._source_id:
            self.router.start(self._source_id)
        

class Join(Node):

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
        if self.left:
            self.left.start()
        self.right.start()

    def inject(self, records):
        # XXX improve here
        self.left.inject(records)
        # XXX right ?

    def dump(self, indent=0):
        self.left.dump(indent+1)
        print ' ' * indent * 4, "JOIN", self.predicate
        self.right.dump(indent+1)

    def left_callback(self, record):
        try:
            if not record:
                self.left_done = True
                if self.right_done:
                    self.callback(None) 
                return
            # New result from the left operator
            if self.right_done:
                if self.left_table:
                    self.process_left_table()
                    self.left_table = []
                self.process_left_record(record)
            else:
                self.left_table.append(record)
        except Exception, e:
            print "Exception during JoinNode::left_callback(%r): %s" % (record, e)
            traceback.print_exc()

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
            print "Exception during JoinNode::right_callback(%r): %s" % (record, e)
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

    def inject(self, records):
        # XXX improve here
        self.node.inject(records)

    def callback(self, record):

        def local_projection(record, fields):
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

        try:
            if not record:
                self.callback(record)
                return
            ret = local_projection(record, self.fields)
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
            child.callback = lambda record: self.child_callback(i, record)
            self.child_status += i
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
        # Loop through children
        for i, child in enumerate(self.children):
            ast = child.root
            # We suppose all results have the same shape, and that we have at
            # least one result
            if ast.query.fact_table not in self.parent_output[0]:
                raise Exception, "Missing primary key information in parent for child %s" % ast.query.fact_table

            # We now inject the information in the ast 
            #ast.inject([row[ast.query.fact_table] for row in self.parent_output])

            # Create a JOiN node
            key = None # XXX
            join = Join([], child, key) # XXX + modified JOiN to accept array

            # Update its query
            fact_table = ast.query.fact_table
            filters = ast.query.filters.union(ast.query.filters)
            fields = ast.query.fields.union(ast.query.fields)
            join.query = Query(fact_table=fact_table, filters=filters, fields=fields)

            # This JOiN node becomes the new child
            self.children[i] = join
            join.callback = lambda record: self.child_callback(i, record)

            # Can itself be a subquery
            # Collect keys from parent results
            parent_keys = []
            for o in self.parent_output:
                if self.parent.query.fact_table in o:
                    # o[method] can be :
                    # - an id (1..)
                    # - an object (1..1)
                    # - a list of id (1..N)
                    # - a list of objects (1..N)
                    if isinstance(o[ast.query.fact_table], list):
                        # We inspected each returned object or key
                        for x in o[ast.query.fact_table]:
                            if isinstance(x, dict):
                                # - get key from metadata and add it
                                # - What to do with remaining fields, we
                                #   might not be able to get them
                                #   somewhere else
                                raise Exception, "returning subobjects not implemented (yet)."
                            else:
                                parent_keys.append(x)
                    else:
                        # 1..1
                        raise Exception, "1..1 relationships are not implemented (yet)."
                    parent_keys.extend(o['key']) # 1..N

            # Add filter on method key
            keys = self.router.metadata_get_keys(ast.query.fact_table)
            if not keys:
                raise Exception, "Cannot complete query: submethod %s has no key" % method
            key = list(keys).pop()
            if ast.query.filters.has_key(key):
                raise Exception, "Filters on keys are not allowed (yet) for subqueries"
            # XXX careful frozen sets !!
            ast.query.filters.add(Predicate(key, '=', parent_keys))

        # Run child nodes
        #print "I: running subquery children"
        for child in self.children:
            child.start()

    def child_done(self, child_id):
        self.child_status -= child_id
        if self.child_status == 0:
            for o in self.parent_output:
                self.callback(o)
            self.callback(None)

    def child_callback(self, child_id, record):
        if not record:
            self.child_done(child_id)
            return
        self.child_results[child_id].append(record)
        # merge results in parent
            


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
    def __init__(self, router=None):
        # Empty request
        self.root = None
        self.router = router

    def _get(self):
        return self.root._get()
        
    def get(self):
        return list(self._get())

    def From(self, table, fields):
        """
        """
        if self.root:
            raise ValueError('AST already initialized')

        self.query = Query('get', table.name, [], {}, fields)
        node = self.router.get_gateway(table.platform, self.query)
        node.source_id = self.router.sourcemgr.append(node)
        self.root = node
        self.root.callback = self.callback
        return self

    # TODO Can we use decorators for such functions ?
    def join(self, ast, predicate):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = Join(old_root, ast.root, predicate)

        fact_table = old_root.query.fact_table
        filters = old_root.query.filters.union(ast.root.query.filters)
        fields = old_root.query.fields.union(ast.root.query.fields)
        self.root.query = Query(fact_table=fact_table, filters=filters, fields=fields)

        self.root.callback = old_root.callback

        return self

    def projection(self, fields):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = Projection(old_root, fields)

        fact_table = old_root.query.fact_table
        filters = old_root.query.filters
        fields = fields
        self.root.query = Query(fact_table=fact_table, filters=filters, fields=fields)

        self.root.callback = old_root.callback

        return self

    def selection(self, filters):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = Selection(old_root, filters)

        fact_table = old_root.query.fact_table
        filters = old_root.query.filters.union(filters)
        fields = old_root.query.fields
        self.root.query = Query(fact_table=fact_table, filters=filters, fields=fields)

        self.root.callback = old_root.callback

        return self

    def subquery(self, children_ast):
        if not self.root:
            raise ValueError('AST not initialized')

        old_root = self.root

        self.root = SubQuery(self.router, old_root, children_ast)

        fact_table = old_root.query.fact_table
        filters = old_root.query.filters
        fields = old_root.query.fields
        self.root.query = Query(fact_table=fact_table, filters=filters, fields=fields)

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

