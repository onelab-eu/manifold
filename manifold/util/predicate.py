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

from types                      import StringTypes
from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg
)

from manifold.util.log          import Log 
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
            args: You may pass:
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
        elif len(args) == 1 and isinstance(args[0], (tuple, list)) and len(args[0]) == 3:
            key, op, value = args[0]
        elif len(args) == 1 and isinstance(args[0], Predicate):
            key, op, value = args[0].get_tuple()
        else:
            raise Exception, "Bad initializer for Predicate (args = %r)" % args

        if isinstance(value, list): value = tuple(value)
        assert not isinstance(key,   (list, frozenset, dict, set)), "Invalid key type (type = %r)"   % type(key)
        assert not isinstance(value, (list, frozenset, dict, set)), "Invalid value type (type = %r)" % type(value)

        self.set_key(key)

        if isinstance(op, StringTypes):
            op = op.upper()

        if op in self.operators.keys():
            self.op = self.operators[op]
        elif op in self.operators_short.keys():
            self.op = self.operators_short[op]
        else:
            self.op = op

        self.set_value(value)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Predicate.
        """
        return "%s %s %r" % self.get_str_tuple() 

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Predicate.
        """
        return "Predicate<%s>" % self.__str__()

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
            The left operand of this Predicate. 
        """
        assert isinstance(self.key, tuple), "Invalid self.key = %s" % self.key
        if len(self.key) == 1:
            (key,) = self.key
            assert isinstance(key, StringTypes), "Invalid key %s" % key
        else:
            key = self.key
        return key
    
    def set_key(self, key):
        """
        Set the left operand of this Predicate.
        Params:
            key: The new left operand (tuple of String or String instance).
        """
        if not isinstance(key, tuple):
            assert isinstance(key, StringTypes), "Invalid key %s" % key
            self.key = (key,)
        else:
            self.key = key

    def get_op(self):
        """
        Returns:
            The operator related to this Predicate.
        """
        return self.op

    def get_value(self):
        """
        Returns:
            The right operand of this Predicate. 
            It may be a literal or a tuple of literal (made of at least 2 literals).
        """
        assert isinstance(self.value, tuple), "Invalid self.value = %s" % self.value # DEBUG
        if len(self.value) == 1 and self.get_op() != included:
            (value,) = self.value
        else:
            value = self.value
        return value

    def set_value(self, value):
        """
        Set the left operand of this Predicate.
        Params:
            value: The new right operand (tuple of literals or a literal) 
        """
        if not isinstance(value, tuple):
            self.value = (value,)
        else:
            assert not isinstance(value, (list, set, frozenset)), "Use a tuple instead"
            self.value = value

    @returns(tuple)
    def get_tuple(self):
        """
        Returns:
            The tuple representing this Predicate.
        """
        return (self.get_key(), self.get_op(), self.get_value())

    @returns(StringTypes)
    def get_str_op(self):
        """
        Returns:
            The String representing the operator involved in this Predicate.
        """
        op_str = [s for s, op in self.operators.iteritems() if op == self.get_op()]
        return op_str[0]

    @returns(tuple)
    def get_str_tuple(self):
        """
        Returns:
            A tuple made of three String instances respectively corresponding
            to the left operand, the operator, and the right operand of this
            Predicate instance.
        """
        key = str(self.get_key())
        value = self.get_value()
        if isinstance(value, tuple):
            value = "[%s]" % ", ".join([repr(v) for v in value])
        else:
            value = "%s" % value
        return (key, self.get_str_op(), value)

    @returns(list)
    def to_list(self):
        return list(self.get_str_tuple())

    @returns(bool)
    def match(self, dic, ignore_missing = False):
        """
        Test whether a dictionnary (related to a Record) satisfies or not
        this Predicate.
        Args:
            dic: A dictionnary instance reprensenting the Record.
            ignore_missing: If the Record does not provide every fields
                involved in this Predicate, we can either deny this
                Record (ignore_missing == False) or either accept it
                (ignore_missing == True)
        Returns:
            True iif the record satisfies this Predicate.
        """
        key   = self.get_key()
        op    = self.get_op()
        value = self.get_value()

        if isinstance(key, tuple):
            print "PREDICATE MATCH", key
            print dic
            print "-----------------------------"

        assert isinstance(key, StringTypes), "key tuple not supported"

        # Can we match ?
        if key not in dic:
            return ignore_missing

        if op == eq:
            if isinstance(value, tuple):
                return (dic[key] in value)
            else:
                return (dic[key] == value)
        elif op == ne:
            if isinstance(value, tuple):
                return (dic[key] not in value)
            else:
                return (dic[key] != value)
        elif op == lt:
            if isinstance(value, StringTypes):
                # prefix match
                return dic[key].startswith("%s." % value)
            else:
                return (dic[key] < value)
        elif op == le:
            if isinstance(value, StringTypes):
                return dic[key] == value or dic[key].startswith("%s." % value)
            else:
                return (dic[key] <= value)
        elif op == gt:
            if isinstance(value, StringTypes):
                # prefix match
                return value.startswith("%s." % dic[key])
            else:
                return (dic[key] > value)
        elif op == ge:
            if isinstance(value, StringTypes):
                # prefix match
                return dic[key] == value or value.startswith("%s." % dic[key])
            else:
                return (dic[key] >= value)
        elif op == and_:
            assert len(value) == 1
            return (dic[key] & value)
        elif op == or_:
            assert len(value) == 1
            return (dic[key] | value)
        elif op == contains:
            method, subfield = key.split(".", 1)
            return not not [ x for x in dic[method] if x[subfield] == value] 
        elif op == included:
            return dic[key] in value
        else:
            raise Exception, "Unexpected table format: %r" % dic

    def filter(self, dic):
        """
        Filter dic according to the current predicate.
        """
        key = self.get_key()

        if "." in key:
            # users.hrn
            method, subfield = key.split(".", 1)
            if not method in dic:
                return None # XXX

            if isinstance(dic[method], dict):
                # We have a 1..1 relationship: apply the same filter to the dict
                subpred = Predicate(subfield, self.get_op(), self.get_value())
                match = subpred.match(dic[method])
                return dic if match else None

            elif isinstance(dic[method], (list, tuple)):
                # 1..N relationships
                match = False
                if self.get_op() == contains:
                    return dic if self.match(dic) else None
                else:
                    subpred = Predicate(subfield, self.get_op(), self.get_value())
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

    @returns(set)
    def get_field_names(self):
        """
        Returns:
            A set of Strings corresponding to each field names
            involved in the left operand of this Predicate.
        """
        return set(self.key) # do not use self.get_key()

    @returns(set)
    def get_value_names(self):
        """
        Returns:
            A set of literals (int, String...) corresponding to
            each value involved in the left operand of this Predicate.
        """
        return set(self.value) # do not use self.get_value()

    @returns(bool)
    def has_empty_value(self):
        """
        Returns:
            True iif the right part (value) of this Predicate is not initialized.
        """
        return len(self.value) == 0 # do not use self.get_value()
