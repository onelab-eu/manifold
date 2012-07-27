#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools

class ASTNode(object):
    def __init__(self):
        pass

    def _get(self):
        yield None

class ASTBinaryNode(ASTNode):
    def __init__(self, left, right):
        self._left = left
        self._right = right
        

class ASTUnaryNode(ASTNode):
    def __init__(self, node):
        self._node = node

    def _get(self):
        return self._node._get()

class ASTLeafNode(ASTNode):
    def __init__(self):
        pass



class ASTFrom(ASTLeafNode):
    def __init__(self, node):
        super(ASTFrom, self).__init__()
        self._node = node

    def dump(self, indent):
        print ' ' * indent * 4, "FROM [%s]" % self._node

    def _get(self):
        return self._node._get()


class ASTJoin(ASTBinaryNode):
    def __init__(self, left, right, predicate):
        super(ASTJoin, self).__init__(left, right)
        self.predicate = predicate

    def dump(self, indent):
        self._left.dump(indent+1)
        print ' ' * indent * 4, "JOIN"
        self._right.dump(indent+1)

    def _get(self):
        left = self._left._get()
        right = self._right._get()
        # Let's build a map according to predicate = simple field name to begin with
        print "Building lookup on predicate '%s' for JOIN" % self.predicate
        rmap = {}
        cptr = 0
        for r in right:
            cptr += 1
            if self.predicate not in r or not r[self.predicate]:
                # We skip records with missing join information
                continue
            rmap[r[self.predicate]] = r
        #print "=================================="
        try:
            cptl = 0
            for l in left:
                cptl += 1
                # We cannot join because the left has no key
                if self.predicate not in l:
                    yield l
                    continue
                if l[self.predicate] in rmap:
                    l.update(rmap[l[self.predicate]])
                    del rmap[l[self.predicate]]
                yield l
            # Handling remaining values from JOIN
            # XXX This is not a left join !!
            #for r in rmap.values():
            #    yield r
            #print "left[%s] = %d, right[%s] = %d" % (self._left._node, cptl, self._right._node, cptr)
        except Exception, e:
            raise Exception, "WEIRD %s" %  e

class ASTProjection(ASTUnaryNode):
    def __init__(self, node, field):
        super(ASTProjection, self).__init__(node)
        self._field = field

    def dump(self, indent):
        print ' ' * indent * 4, "SELECT [%s]" % self._field
        self._node.dump(indent+1)

    def _get(self):
        for row in self._node._get():
            res = {}
            for k, v in row.items():
                if k in self._field:
                    res[k] = v
            yield res
    

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


class ASTSelection(ASTUnaryNode):
    def __init__(self, node, filters):
        super(ASTSelection, self).__init__(node)
        self._filters = filters

    def dump(self, indent):
        print ' ' * indent * 4, "WHERE [%r]"  % self._filters
        self._node.dump(indent+1)

    def _get(self):
        for row in self._node._get():
            if not match_filters(row, self._filters):
                continue
            yield row
                



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

    def From(self, table):
        """
        """
        if self._root:
            raise ValueError('AST already initialized')

        n = ASTFrom(table)
        self._root = n
        return self

    def join(self, ast, predicate):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = ASTJoin(self._root, ast.get_root(), predicate)
        self._root = n
        return self

    def projection(self, field):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = ASTProjection(self._root, field)
        self._root = n
        return self

    def selection(self, filter):
        """
        """
        if not self._root:
            raise ValueError('AST not initialized')

        n = ASTSelection(self._root, filter)
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

