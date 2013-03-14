#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Query representation
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                      import StringTypes
from manifold.core.filter         import Filter, Predicate
from manifold.util.frozendict     import frozendict
from manifold.util.type           import returns, accepts
from copy                       import deepcopy

class ParameterError(StandardError): pass

class Query(object):
    """
    Implements a TopHat query.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.
    """

    def __init__(self, *args, **kwargs):

        # Initialize optional parameters
        self.filters = Filter([])
        self.params  = {}
        self.fields  = set([])
        self.ts      = "now" 

        #l = len(kwargs.keys())
        len_args = len(args)

        if len(args) == 1 and isinstance(args[0], Query):
            # Copy
            return deepcopy(args[0])

        # Initialization from a tuple

        if len_args in range(2, 7) and type(args) == tuple:
            # Note: range(x,y) <=> [x, y[

            # XXX UGLY
            if len_args == 3:
                self.action = 'get'
                self.params = {}
                self.ts     = 'now'
                self.fact_table, self.filters, self.fields = args
            elif len_args == 4:
                self.fact_table, self.filters, self.params, self.fields = args
                self.action = 'get'
                self.ts     = 'now'
            else:
                self.action, self.fact_table, self.filters, self.params, self.fields, self.ts = args

        # Initialization from a dict (action & fact_table are mandatory)
        elif "fact_table" in kwargs:
            if "action" in kwargs:
                self.action = kwargs["action"]
                del kwargs["action"]
            else:
                print "W: defaulting to get action"
                self.action = "get"

            self.fact_table = kwargs["fact_table"]
            del kwargs["fact_table"]

            if "filters" in kwargs:
                self.filters = kwargs["filters"]
                del kwargs["filters"]
            else:
                self.filters = Filter([])

            if "fields" in kwargs:
                self.fields = set(kwargs["fields"])
                del kwargs["fields"]
            else:
                self.fields = set([])

            # "update table set x = 3" => params == set
            if "params" in kwargs:
                self.params = kwargs["params"]
                del kwargs["params"]
            else:
                self.params = {}

            if "ts" in kwargs:
                self.ts = kwargs["ts"]
                del kwargs["ts"]
            else:
                self.ts = "now" 

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
        #else:
        #        raise ParameterError, "No valid constructor found for %s : args = %r" % (self.__class__.__name__, args)

        if not self.filters: self.filters = Filter([])
        if not self.params:  self.params  = {}
        if not self.fields:  self.fields  = set([])
        if not self.ts:      self.ts      = "now" 

        if isinstance(self.filters, list):
            f = self.filters
            self.filters = Filter([])
            for x in f:
                pred = Predicate(x)
                self.filters.add(pred)

        if isinstance(self.fields, list):
            self.fields = set(self.fields)

        for field in self.fields:
            if not isinstance(field, StringTypes):
                raise TypeError("Invalid field name %s (string expected, got %s)" % (field, type(field)))

    @returns(StringTypes)
    def __str__(self):
        return "SELECT %s FROM %s WHERE %s" % (
            ", ".join(self.get_select()),
            self.get_from(),
            self.get_where()
        )

    @returns(StringTypes)
    def __repr__(self):
        return self.__str__()

    def __key(self):
        return (self.action, self.fact_table, self.filters, frozendict(self.params), frozenset(self.fields))

    def __hash__(self):
        return hash(self.__key())

    @returns(StringTypes)
    def get_action(self):
        return self.action

    @returns(frozenset)
    def get_select(self):
        return frozenset(self.fields)

    @returns(StringTypes)
    def get_from(self):
        return self.fact_table

    @returns(Filter)
    def get_where(self):
        return self.filters

    @returns(dict)
    def get_params(self):
        return self.params

    @returns(StringTypes)
    def get_ts(self):
        return self.ts

    #--------------------------------------------------------------------------- 
    # Checks
    #--------------------------------------------------------------------------- 

    def make_filters(self, filters):
        return Filter(filters)

    def make_fields(self, fields):
        if isinstance(fields, (list, tuple)):
            return set(fields)
        else:
            raise Exception, "Invalid field specification"

    #--------------------------------------------------------------------------- 
    # LINQ-like syntax
    #--------------------------------------------------------------------------- 

    @classmethod
    def action(self, action, fact_table):
        query = Query()
        query.action = 'get'
        query.fact_table = fact_table
        return query

    @classmethod
    def get(self, fact_table): return self.action('get', fact_table)

    @classmethod
    def update(self, fact_table): return self.action('update', fact_table)
    
    @classmethod
    def create(self, fact_table): return self.action('create', fact_table)
    
    @classmethod
    def delete(self, fact_table): return self.action('delete', fact_table)
    
    @classmethod
    def execute(self, fact_table): return self.action('execute', fact_table)

    def filters(self, filters):
        self.filters = self.make_filters(filters)
        return self

    def fields(self, fields):
        self.fields = self.make_fields(fields)
