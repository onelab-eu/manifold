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
import copy

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
        self.clear()
    
        #l = len(kwargs.keys())
        len_args = len(args)

        if len(args) == 1:
            if isinstance(args[0], dict):
                kwargs = args[0]
                args = []

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

        # Initialization from a dict
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

    def copy(self):
        return copy.deepcopy(self)

    def clear(self):
        self.action = 'get'
        self.fact_table = None
        self.filters = Filter([])
        self.params  = {}
        self.fields  = set([])
        self.ts      = "now" 

    @returns(StringTypes)
    def __str__(self):
        return "SELECT %s FROM %s WHERE %s" % (
            ", ".join(self.get_select()) if self.get_select() else '*',
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

#DEPRECATED#
#DEPRECATED#    def make_filters(self, filters):
#DEPRECATED#        return Filter(filters)
#DEPRECATED#
#DEPRECATED#    def make_fields(self, fields):
#DEPRECATED#        if isinstance(fields, (list, tuple)):
#DEPRECATED#            return set(fields)
#DEPRECATED#        else:
#DEPRECATED#            raise Exception, "Invalid field specification"

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

    def filter_by(self, *args):
        print "query::filter_by"
        if len(args) == 1:
            filters = args[0]
            if not isinstance(filters, (set, list, tuple, Filter)):
                filters = [filters]
            for predicate in filters:
                self.filters.add(predicate)
        elif len(args) == 3:
            predicate = Predicate(*args)
            self.filters.add(predicate)
        else:
            raise Exception, 'Invalid expression for filter'
        return self
            
    def select(self, fields):
        if not isinstance(fields, (set, list, tuple)):
            fields = [fields]
        for field in fields:
            self.fields.add(field)
        return self

    def set(self, params):
        self.params.update(params)
        return self

class AnalyzedQuery(Query):

    # XXX we might need to propagate special parameters sur as DEBUG, etc.

    def __init__(self, query=None):
        self.clear()
        self._analyzed = None
        if query:
            self.analyze(query)

    @returns(StringTypes)
    def __str__(self):
        out = []
        out.append("SELECT %s FROM %s WHERE %s" % (
            ", ".join(self.get_select()),
            self.get_from(),
            self.get_where()
        ))
        for method, subquery in self.subqueries():
            out.append('  [SQ : %s] %s' % (method, str(subquery)))
        return "\n".join(out)

    def clear(self):
        super(AnalyzedQuery, self).clear()
        self._subqueries = {}

    def subquery(self, method):
        # Allows for the construction of a subquery
        if not method in self._subqueries:
            analyzed_query = AnalyzedQuery()
            analyzed_query.action = self._analyzed.action
            analyzed_query.fact_table = method
            self._subqueries[method] = analyzed_query
        return self._subqueries[method]

    def subqueries(self):
        for method, subquery in self._subqueries.iteritems():
            yield (method, subquery)

    def filter_by(self, filters):
        if not filters: return self
        if not isinstance(filters, (set, list, tuple, Filter)):
            filters = [filters]
        for predicate in filters:
            if '.' in predicate.key:
                method, subkey = pred.key.split('.', 1)
                sub_pred = Predicate(subkey, pred.op, pred.value)
                self.subquery(method).filter_by(sub_pred)
            else:
                super(AnalyzedQuery, self).filter_by(predicate)
        return self

    def select(self, fields):
        if not isinstance(fields, (set, list, tuple)):
            fields = [fields]
        for field in fields:
            if '.' in field:
                method, subfield = field.split('.', 1)
                self.subquery(method).select(subfield)
            else:
                super(AnalyzedQuery, self).select(field)
        return self

    def set(self, params):
        for param, value in self.params.items():
            if '.' in param:
                method, subparam = param.split('.', 1)
                self.subquery(method).set({subparam: value})
            else:
                super(AnalyzedQuery, self).set({param: value})
        return self
        
    def analyze(self, query):
        self._analyzed = query
        self.clear()
        self.action = query.action
        self.fact_table = query.fact_table
        self.filter_by(query.filters)
        self.set(query.params)
        self.select(query.fields)
        self._analyzed = None