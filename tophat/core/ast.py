#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools

#from tophat.core.nodes import *

class Node(object):
    #def __init__(self):
    #    pass

    def set_callback(self, callback):
        self.callback = callback

class FromNode(Node):

    def __init__(self, platform, query, config):
        self.platform = platform
        self.query = query
        self.config = config
        self.do_start = False
        self.started = False
        self.callback = None

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT %r FROM '%s'" % (self.query.fields, self.query.fact_table)

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
        print "JOIN", left, right
        # Parameters
        self.left = left
        self.right = right
        self.predicate = predicate
        # Set up callbacks
        left.callback = self.left_callback
        right.callback = self.right_callback
        # Local variables
        self.right_map = {}
        self.right_done = False
        self.left_done = False
        self.left_table = []

    def start(self):
        self.left.start()
        self.right.start()


    def dump(self, indent):
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
        self.node = node
        self.fields = fields
        # Set up callbacks
        self.node.callback = self.callback

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT [%s]" % self.fields
        self.node.dump(indent+1)

    def start(self):
        self.node.start()

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
        self.node = node
        self.filters = filters

    def dump(self, indent):
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

class SubQuery(Node):

    def __init__(self, router, parent, children):
        self.callback = None
        # for AST
        self.parent = parent
        self.children = children
        # Local storage
        self.parent_output = []
        # Set up callbacks
        parent.callback = self.parent_callback
        for i, child in enumerate(children):
            child.callback = lambda record: self.child_callback(i, record)

    def dump(self, indent):
        self.parent.dump(indent+1)
        if not self.children: return
        print ' ' * (indent+1) * 4, "<subqueries>"
        for child in self.children:
            child.dump(indent+1)

    def start(self):
        self.parent.start()
        for child in self.children:
            child.start()

    def parent_callback(self, record):
        if record:
            print record
            self.parent_output.append(record)
            return
        # When we have received all parent records, we can run children
        self.run_children()

    def run_children(self):
        """
        Modify children queries to take the keys returned by the parent into account
        """
        # Loop through children
        for q in self.children:
            q = q.root
            # Can itself be a subquery
            # Collect keys from parent results
            parent_keys = []
            for o in self.parent_output:
                print "Considering output", o
                if self.parent.query.fact_table in o:
                    # o[method] can be :
                    # - an id (1..)
                    # - an object (1..1)
                    # - a list of id (1..N)
                    # - a list of objects (1..N)
                    if isinstance(o[q.fact_table], list):
                        # We inspected each returned object or key
                        for x in o[q.fact_table]:
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
            keys = self.router.metadata_get_keys(q.fact_table)
            if not keys:
                raise Exception, "Cannot complete query: submethod %s has no key" % method
            key = list(keys).pop()
            if q.filters.has_key(key):
                raise Exception, "Filters on keys are not allowed (yet) for subqueries"
            # XXX careful frozen sets !!
            q.filters.add(Predicate(key, '=', parent_keys))

        # Run child nodes
        # XXX TODO

    def child_callback(self, child_id, record):

        # When we have all results !
        self._callback(merged_parent_record)
            

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
        self.callback = None

    def _get(self):
        return self.root._get()
        
    def get(self):
        return list(self._get())

    def From(self, table, fields):
        """
        """
        if self.root:
            raise ValueError('AST already initialized')

        node = self.router.get_gateway(table.platform, table.name, fields)
        node.source_id = self.router.sourcemgr.append(node)
        self.root = node
        self.root.callback = self.callback
        return self

    def join(self, ast, predicate):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        root_cb = self.root.callback
        print "|||", self.root, ast.root, predicate
        self.root = Join(self.root, ast.root, predicate)
        self.root.callback = root_cb
        return self

    def projection(self, fields):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        root_cb = self.root.callback
        self.root = Projection(self.root, fields)
        self.root.callback = root_cb
        return self

    def selection(self, filters):
        """
        """
        if not self.root:
            raise ValueError('AST not initialized')

        root_cb = self.root.callback
        self.root = Selection(self.root, filters)
        self.root.callback = root_cb
        return self

    def subquery(self, children_ast):
        if not self.root:
            raise ValueError('AST not initialized')

        root_cb = self.root.callback
        self.root = SubQuery(self.router, self.root, children_ast)
        self.root.callback = root_cb
        return self

    def dump(self, indent=0):
        self.root.dump(indent)

    def start(self):
        self.root.start()

    def set_callback(self, callback):
        self.root.callback = callback
        # start sources now !
        self.root.start()

        

def main():
    a = AST().From('A').join(AST().From('B')).projection('c').selection(Eq('c', 'test'))
    a.dump()
#    a.swaphead()
#    a.dump()

if __name__ == "__main__":
    main()

