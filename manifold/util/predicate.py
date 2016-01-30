#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Predicate: 
# Define a condition to join for example to Table instances.
# If this condition involves several fields, you may define a
# single Predicate using tuple of fields. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from __future__ import print_function

# python3
try:    from types import StringTypes
except: StringTypes = str

from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg
)

from manifold.util.type         import returns, accepts 

# Define the inclusion operators
class contains(type): pass
class included(type): pass

# New modifier: { contains 
class Predicate:

    operators = {
        '=='       : eq,
        '!='       : ne,
        '<'        : lt,
        '<='       : le,
        '>'        : gt,
        '>='       : ge,
        '&&'       : and_,
        '||'       : or_,
        'CONTAINS' : contains,
        'INCLUDED' : included
    }

    operators_short = {
        '=' : eq,
        '~' : ne,
        '<' : lt,
        '[' : le,
        '>' : gt,
        ']' : ge,
        '&' : and_,
        '|' : or_,
        '}' : contains,
        '{' : included
    }

    def __init__(self, *args, **kwargs):
        """
        Build a Predicate instance.
        Args: 
            kwargs: You can pass:
                - 3 args (left, operator, right)
                    left: The left operand (it may be a String instance or a tuple)
                    operator: See Predicate.operators, this is the binary operator
                        involved in this Predicate. 
                    right: The right value (it may be a String instance
                        or a literal (String, numerical value, tuple...))
                - 1 argument (list or tuple), containing three arguments
                  (variable, operator, value)
        """
        if len(args) == 3:
            key, op, value = args
        elif len(args) == 1 and isinstance(args[0], (tuple,list)) and len(args[0]) == 3:
            key, op, value = args[0]
        elif len(args) == 1 and isinstance(args[0], Predicate):
            key, op, value = args[0].get_tuple()
        else:
            raise Exception("Bad initializer for Predicate (args = %r)" % args)

        assert not isinstance(value, (frozenset, dict, set)), "Invalid value type (type = %r)" % type(value)
        if isinstance(value, list):
            value = tuple(value)

        self.key = key
        if isinstance(op, StringTypes):
            op = op.upper()
        if op in self.operators.keys():
            self.op = self.operators[op]
        elif op in self.operators_short.keys():
            self.op = self.operators_short[op]
        else:
            self.op = op

        if isinstance(value, list):
            self.value = tuple(value)
        else:
            self.value = value

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Predicate.
        """
        key, op, value = self.get_str_tuple()
        if isinstance(value, (tuple, list, set, frozenset)):
            value = [repr(v) for v in value]
            value = "[%s]" % ", ".join(value)
        return "%s %s %r" % (key, op, value) 

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Predicate.
        """
        return "Predicate<%s %s %r>" % self.get_str_tuple()

    def __hash__(self):
        """
        Returns:
            The hash of this Predicate (this allows to define set of
            Predicate instances).
        """
        return hash(self.get_tuple())

    @returns(bool)
    def __eq__(self, predicate):
        """
        Returns:
            True iif self == predicate.
        """
        if not predicate:
            return False
        return self.get_tuple() == predicate.get_tuple()

    def get_key(self):
        """
        Returns:
            The left operand of this Predicate. It may be a String
            or a tuple of Strings.
        """
        return self.key
    
    def set_key(self, key):
        """
        Set the left operand of this Predicate.
        Params:
            key: The new left operand.
        """
        self.key = key

    def get_op(self):
        return self.op

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value

    def get_tuple(self):
        return (self.key, self.op, self.value)

    def get_str_op(self):
        # for python3
        try:
            op_str = [s for s, op in self.operators.iteritems() if op == self.op]
        except:
            op_str = [s for s, op in self.operators.items() if op == self.op]
        return op_str[0]

    def get_str_tuple(self):
        return (self.key, self.get_str_op(), self.value,)

    def to_list(self):
        return list(self.get_str_tuple())

    def match(self, dic, ignore_missing=False):
        def _tuple_match(key, value):
            key_list = list(key)
            value_list = list(value)
            
            for i, k in enumerate(key_list):
                if dic.get(k) != value_list[i]:
                    return False
            return True
            
        
        # Can we match ?
        if not isinstance(self.key, tuple) and self.key not in dic:
            return not ignore_missing

        if self.op == eq:
            if isinstance(self.key, tuple):
                return _tuple_match(self.key, self.value)
            else:
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
            #method, subfield = self.key.split('.', 1)
            #return not not [ x for x in dic[method] if x[subfield] == self.value] 
            if '.' in self.key:
                # CONTAINS for objects in other object - Ex: users under an authority
                method, subfield = self.key.split('.', 1)
                return not not [ x for x in dic[method] if x[subfield] == self.value] 
            else:
                # CONTAINS for objects starting with the same name in a hierarchy 
                # Ex: onalb.upmc CONTAINS onelab.upmc.projectX 
                return dic[self.key].startswith('%s.' % self.value)
        elif self.op == included:
            if isinstance(self.key, tuple):
                # The dic needs to match at least one value
                for v in self.value:
                    if _tuple_match(self.key, v):
                        return True
                return False
            else:
                return dic[self.key] in self.value
        else:
            raise Exception("Unexpected table format: %r" % dic)

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
                raise Exception("Unexpected table format: %r", dic)


        else:
            # Individual field operations: this could be simplified, since we are now using operators_short !!
            # XXX match
            print("current predicate", self)
            print("matching", dic)
            print("----")
            return(dic if self.match(dic) else None)

    def get_field_names(self):
        if isinstance(self.key, (list, tuple, set, frozenset)):
            return set(self.key)
        else:
            return set([self.key])

    def get_value_names(self):
        if isinstance(self.value, (list, tuple, set, frozenset)):
            return set(self.value)
        else:
            return set([self.value])

    def has_empty_value(self):
        if isinstance(self.value, (list, tuple, set, frozenset)):
            return not any(self.value)
        else:
            return not self.value

    # XXX IMPORTED FROM ROUTERV2
    @returns(bool)
    def is_composite(self):
        """
        Returns:
            True iif this Predicate instance involves
            a tuple key (and tuple value).
        """ 
        return isinstance(self.get_key(), tuple)

    # XXX IMPORTED FROM ROUTERV2
    def rename(self, aliases):
        if self.is_composite():
            raise NotImplemented
        if self.key in aliases:
            self.key = aliases[self.key]
