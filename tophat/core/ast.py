#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools

from tophat.core.router import THQuery
from tophat.core.nodes import *

class Node(object):
    def __init__(self):
        pass

    def _get(self):
        yield None

class BinaryNode(Node):
    def __init__(self, left, right):
        self._left = left
        self._right = right

#    def children(self):
#        return [self._left, self._right]
        

class UnaryNode(Node):
    def __init__(self, node):
        self._node = node

    def _get(self):
        return self._node._get()

class LeafNode(Node):
    def __init__(self):
        pass


class From(LeafNode):

    def __init__(self, table, fields):
        super(From, self).__init__()
        self.table = table
        self.fields = fields

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT %r FROM '%s'" % (self.fields, self.table)

    def install(self, router, callback):
        node = router.get_gateway(self.table.platform, callback, THQuery('get', self.table.name, [], {}, self.fields))
        router.sourcemgr.append(node)
        return node

class Join(BinaryNode):

    def __init__(self, left, right, predicate):
        super(Join, self).__init__(left, right)
        self._predicate = predicate
        self.left_done = False
        self.right_done = False
        self.right_map = {}

    def install(self, router, callback):
        node = JoinNode(self._predicate, callback)
        node.children = []
        node.children.append(self._left.install(router, node.left_callback))
        node.children.append(self._right.install(router, node.right_callback))
        return node


    def dump(self, indent):
        self._left.dump(indent+1)
        print ' ' * indent * 4, "JOIN", self._predicate
        self._right.dump(indent+1)

class Projection(UnaryNode):

    def __init__(self, node, fields):
        super(Projection, self).__init__(node)
        self._fields = fields

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT [%s]" % self._fields
        self._node.dump(indent+1)

    def install(self, router, callback):
        node = ProjectionNode(self._fields, callback)
        node.children = [self._node.install(router, node.callback)]
        return node

#    def _get(self):
#        for row in self._node._get():
#            res = {}
#            for k, v in row.items():
#                if k in self._fields:
#                    res[k] = v
#            yield res


class Selection(UnaryNode):

    def __init__(self, node, filters):
        super(Selection, self).__init__(node)
        self._filters = filters

    def dump(self, indent):
        print ' ' * indent * 4, "WHERE %s"  % self._filters
        self._node.dump(indent+1)

    def install(self, router, callback):
        node = SelectionNode(self._filters, callback)
        node.children = [self._node.install(router, node.callback)]
        return node

#    def _get(self):
#        for row in self._node._get():
#            if not match_filters(row, self._filters):
#                continue
#            yield row
    

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


class AST(object):                  # = Queryable
    def __init__(self):
        # Empty request
        self._root = None

    def _get(self):
        return self._root._get()
        
    def get(self):
        return list(self._get())

    def From(self, table, fields):
        """
        """
        if self._root:
            raise ValueError('AST already initialized')

        n = From(table, fields)
        self._root = n
        return self

    def join(self, ast, predicate):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = Join(self._root, ast.get_root(), predicate)
        self._root = n
        return self

    def projection(self, fields):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = Projection(self._root, fields)
        self._root = n
        return self

    def selection(self, filters):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = Selection(self._root, filters)
        self._root = n
        return self

    def dump(self):
        self._root.dump(indent=0)

    def get_root(self):
        return self._root

        

def main():
    a = AST().From('A').join(AST().From('B')).projection('c').selection(Eq('c', 'test'))
    a.dump()
    a.swaphead()
    a.dump()

if __name__ == "__main__":
    main()

