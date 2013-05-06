from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg
    )
from manifold.util.misc import contains
from types import StringTypes

# New modifier: { contains 
class Predicate:

    operators = {
        "=="       : eq,
        "!="       : ne,
        "<"        : lt,
        "<="       : le,
        ">"        : gt,
        ">="       : ge,
        "&&"       : and_,
        "||"       : or_,
        "contains" : contains
    }

    operators_short = {
        "=" : eq,
        "~" : ne,
        "<" : lt,
        "[" : le,
        ">" : gt,
        "]" : ge,
        "&" : and_,
        "|" : or_,
        "}" : contains
    }

    def __init__(self, *args, **kwargs):
        """
        \brief Build a predicate (left, operator, right)
        \param You can pass:
            - three args (left, operator, right)
            - one argument (list or tuple) containing three elements (variable, operator, value)
            "operator" is a String defined in operators or in operators_short and refers
                tMao a binary operation.
            "left" and "right" refers to a variable/constant involved in the Predicate.
        """
        if len(args) == 3:
            key, op, value = args
        elif len(args) == 1 and isinstance(args[0], (tuple,list)) and len(args[0]) == 3:
            key, op, value = args[0]
        elif len(args) == 1 and isinstance(args[0], Predicate):
            key, op, value = args[0].get_tuple()
        else:
            raise Exception, "Bad initializer for Predicate"
        self.key = key
        if op in self.operators.keys():
            self.op = self.operators[op]
        elif op in self.operators_short.keys():
            self.op = self.operators_short[op]
        else:
            self.op = op
        if isinstance(value, (list, set)):
            self.value = tuple(value)
        else:
            self.value = value

    def __str__(self):
        return "Pred(%s, %s, %s)" % self.get_str_tuple()

    def __repr__(self):
        return self.__str__() 

    def __hash__(self):
        return hash(self.get_tuple())

    def get_tuple(self):
        return (self.key, self.op, self.value)

    def get_str_op(self):
        op_str = [s for s, op in self.operators.iteritems() if op == self.op]
        return op_str[0]

    def get_str_tuple(self):
        return (self.key, self.get_str_op(), self.value,)

    def match(self, dic, ignore_missing=False):
        if isinstance(self.key, tuple):
            print "PREDICATE MATCH", self.key
            print dic
            print "-----------------------------"
        
        # Can we match ?
        if self.key not in dic:
            return ignore_missing

        if self.op == eq:
            if isinstance(self.value, list):
                return (dic[self.key] in self.value) # array ?
            else:
                return (dic[self.key] == self.value)
        elif self.op == ne:
            if isinstance(self.value, list):
                return (dic[self.key] not in self.value) # array ?
            else:
                return (dic[self.key] != self.value) # array ?
        elif self.op == lt:
            if isinstance(self.value, StringTypes):
                # prefix match
                return dic[self.key].startswith('%s.' % self.value)
            else:
                return (dic[self.key] < self.value)
        elif self.op == le:
            if isinstance(self.value, StringTypes):
                return dic[self.key] == self.value or dic[self.key].startswith('%s.' % self.value)
            else:
                return (dic[self.key] <= self.value)
        elif self.op == gt:
            if isinstance(self.value, StringTypes):
                # prefix match
                return self.value.startswith('%s.' % dic[self.key])
            else:
                return (dic[self.key] > self.value)
        elif self.op == ge:
            if isinstance(self.value, StringTypes):
                # prefix match
                return dic[self.key] == self.value or self.value.startswith('%s.' % dic[self.key])
            else:
                return (dic[self.key] >= self.value)
        elif self.op == and_:
            return (dic[self.key] & self.value) # array ?
        elif self.op == or_:
            return (dic[self.key] | self.value) # array ?
        elif self.op == contains:
            method, subfield = self.key.split('.', 1)
            return not not [ x for x in dic[method] if x[subfield] == self.value] 
        else:
            raise Exception, "Unexpected table format: %r", dic

    def filter(self, dic):
        """
        Filter dic according to the current predicate.
        """

        if '.' in self.key:
            # users.hrn
            method, subfield = self.key.split('.', 1)
            if not method in dic:
                return None # XXX

            if isinstance(dic[method], dict):
                # We have a 1..1 relationship: apply the same filter to the dict
                subpred = Predicate(subfield, self.op, self.value)
                match = subpred.match(dic[method])
                return dic if match else None

            elif isinstance(dic[method], (list, tuple)):
                # 1..N relationships
                match = False
                if self.op == contains:
                    return dic if self.match(dic) else None
                else:
                    subpred = Predicate(subfield, self.op, self.value)
                    dic[method] = subpred.filter(dic[method])
                    return dic
            else:
                raise Exception, "Unexpected table format: %r", dic


        else:
            # Individual field operations: this could be simplified, since we are now using operators_short !!
            # XXX match
            print "current predicate", self
            print "matching", dic
            print "----"
            return dic if self.match(dic) else None

