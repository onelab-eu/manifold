#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Query representation
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Thierry Parmentelat <thierry.parmentelat@inria.fr>

from types                      import StringTypes
from manifold.core.filter         import Filter, Predicate
from manifold.util.frozendict     import frozendict
from manifold.util.type           import returns, accepts
import copy

import json
import uuid

def uniqid (): 
    return uuid.uuid4().hex

debug=False
debug=True

class ParameterError(StandardError): pass

class Query(object):
    """
    Implements a TopHat query.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.
    """

    #--------------------------------------------------------------------------- 
    # Constructor
    #--------------------------------------------------------------------------- 

    def __init__(self, *args, **kwargs):

        self.query_uuid = uniqid()

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
                self.timestamp     = 'now'
                self.object, self.filters, self.fields = args
            elif len_args == 4:
                self.object, self.filters, self.params, self.fields = args
                self.action = 'get'
                self.timestamp     = 'now'
            else:
                self.action, self.object, self.filters, self.params, self.fields, self.timestamp = args

        # Initialization from a dict
        elif "object" in kwargs:
            if "action" in kwargs:
                self.action = kwargs["action"]
                del kwargs["action"]
            else:
                print "W: defaulting to get action"
                self.action = "get"


            self.object = kwargs["object"]
            del kwargs["object"]

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

            if "timestamp" in kwargs:
                self.timestamp = kwargs["timestamp"]
                del kwargs["timestamp"]
            else:
                self.timestamp = "now" 

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
        #else:
        #        raise ParameterError, "No valid constructor found for %s : args = %r" % (self.__class__.__name__, args)

        if not self.filters: self.filters = Filter([])
        if not self.params:  self.params  = {}
        if not self.fields:  self.fields  = set([])
        if not self.timestamp:      self.timestamp      = "now" 

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

    #--------------------------------------------------------------------------- 
    # Helpers
    #--------------------------------------------------------------------------- 

    def copy(self):
        return copy.deepcopy(self)

    def clear(self):
        self.action = 'get'
        self.object = None
        self.filters = Filter([])
        self.params  = {}
        self.fields  = set([])
        self.timestamp  = "now" 
        self.timestamp  = 'now' # ignored for now

    @returns(StringTypes)
    def __str__(self):
        return "SELECT %(select)s%(from)s%(where)s%(at)s" % {
            "select": ", ".join(self.get_select())          if self.get_select()    else "*",
            "from"  : "\n  FROM  %s" % self.get_from(),
            "where" : "\n  WHERE %s" % self.get_where()     if self.get_where()     else "",
            "at"    : "\n  AT    %s" % self.get_timestamp() if self.get_timestamp() else ""
        }

    @returns(StringTypes)
    def __repr__(self):
        return "SELECT %s FROM %s WHERE %s" % (
            ", ".join(self.get_select()) if self.get_select() else '*',
            self.get_from(),
            self.get_where()
        )

    def __key(self):
        return (self.action, self.object, self.filters, frozendict(self.params), frozenset(self.fields))

    def __hash__(self):
        return hash(self.__key())

    #--------------------------------------------------------------------------- 
    # Conversion
    #--------------------------------------------------------------------------- 

    def to_dict(self):
        return {
            'action': self.action,
            'object': self.object,
            'timestamp': self.timestamp,
            'filters': self.filters,
            'params': self.params,
            'fields': list(self.fields)
        }

    def to_json (self, analyzed_query=None):
        query_uuid=self.query_uuid
        a=self.action
        o=self.object
        t=self.timestamp
        f=json.dumps (self.filters.to_list())
        p=json.dumps (self.params)
        c=json.dumps (list(self.fields))
        # xxx unique can be removed, but for now we pad the js structure
        unique=0

        if not analyzed_query:
            aq = 'null'
        else:
            aq = analyzed_query.to_json()
        sq="{}"
        
        result= """ new ManifoldQuery('%(a)s', '%(o)s', '%(t)s', %(f)s, %(p)s, %(c)s, %(unique)s, '%(query_uuid)s', %(aq)s, %(sq)s)"""%locals()
        if debug: print 'ManifoldQuery.to_json:',result
        return result
    
    # this builds a ManifoldQuery object from a dict as received from javascript through its ajax request 
    # we use a json-encoded string - see manifold.js for the sender part 
    # e.g. here's what I captured from the server's output
    # manifoldproxy.proxy: request.POST <QueryDict: {u'json': [u'{"action":"get","object":"resource","timestamp":"latest","filters":[["slice_hrn","=","ple.inria.omftest"]],"params":[],"fields":["hrn","hostname"],"unique":0,"query_uuid":"436aae70a48141cc826f88e08fbd74b1","analyzed_query":null,"subqueries":{}}']}>
    def fill_from_POST (self, POST_dict):
        try:
            json_string=POST_dict['json']
            dict=json.loads(json_string)
            for (k,v) in dict.iteritems(): 
                setattr(self,k,v)
        except:
            print "Could not decode incoming ajax request as a Query, POST=",POST_dict
            if (debug):
                import traceback
                traceback.print_exc()

    #--------------------------------------------------------------------------- 
    # Accessors
    #--------------------------------------------------------------------------- 

    @returns(StringTypes)
    def get_action(self):
        return self.action

    @returns(frozenset)
    def get_select(self):
        return frozenset(self.fields)

    @returns(StringTypes)
    def get_from(self):
        return self.object

    @returns(Filter)
    def get_where(self):
        return self.filters

    @returns(dict)
    def get_params(self):
        return self.params

    @returns(StringTypes)
    def get_timestamp(self):
        return self.timestamp

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
    def action(self, action, object):
        query = Query()
        query.action = action
        query.object = object
        return query

    @classmethod
    def get(self, object): return self.action('get', object)

    @classmethod
    def update(self, object): return self.action('update', object)
    
    @classmethod
    def create(self, object): return self.action('create', object)
    
    @classmethod
    def delete(self, object): return self.action('delete', object)
    
    @classmethod
    def execute(self, object): return self.action('execute', object)

    def at(self, timestamp):
        self.timestamp = timestamp
        return self

    def filter_by(self, *args):
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
            
    def select(self, fields=None):
        if not fields:
            # Delete all fields
            self.fields = set()
            return self
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

    def __init__(self, query=None, metadata=None):
        self.clear()
        self.metadata = metadata
        if query:
            self.query_uuid = query.query_uuid
            self.analyze(query)
        else:
            self.query_uuid = uniqid()

    @returns(StringTypes)
    def __str__(self):
        out = []
        fields = self.get_select()
        fields = ", ".join(fields) if fields else '*'
        out.append("SELECT %s FROM %s WHERE %s" % (
            fields,
            self.get_from(),
            self.get_where()
        ))
        cpt = 1
        for method, subquery in self.subqueries():
            out.append('  [SQ #%d : %s] %s' % (cpt, method, str(subquery)))
            cpt += 1

        return "\n".join(out)

    def clear(self):
        super(AnalyzedQuery, self).clear()
        self._subqueries = {}

    def subquery(self, method):
        # Allows for the construction of a subquery
        if not method in self._subqueries:
            analyzed_query = AnalyzedQuery(metadata=self.metadata)
            analyzed_query.action = self.action
            try:
                type = self.metadata.get_field_type(self.object, method)
            except ValueError ,e: # backwards 1..N
                type = method
            analyzed_query.object = type
            self._subqueries[method] = analyzed_query
        return self._subqueries[method]

    def get_subquery(self, method):
        return self._subqueries.get(method, None)

    def remove_subquery(self, method):
        del self._subqueries[method]

    def get_subquery_names(self):
        return set(self._subqueries.keys())

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
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
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
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
                self.subquery(method).select(subfield)
            else:
                super(AnalyzedQuery, self).select(field)
        return self

    def set(self, params):
        for param, value in self.params.items():
            if '.' in param:
                method, subparam = param.split('.', 1)
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
                self.subquery(method).set({subparam: value})
            else:
                super(AnalyzedQuery, self).set({param: value})
        return self
        
    def analyze(self, query):
        self.clear()
        self.action = query.action
        self.object = query.object
        self.filter_by(query.filters)
        self.set(query.params)
        self.select(query.fields)

    def to_json (self):
        query_uuid=self.query_uuid
        a=self.action
        o=self.object
        t=self.timestamp
        f=json.dumps (self.filters.to_list())
        p=json.dumps (self.params)
        c=json.dumps (list(self.fields))
        # xxx unique can be removed, but for now we pad the js structure
        unique=0

        aq = 'null'
        sq=", ".join ( [ "'%s':%s" % (object, subquery.to_json())
                  for (object, subquery) in self._subqueries.iteritems()])
        sq="{%s}"%sq
        
        result= """ new ManifoldQuery('%(a)s', '%(o)s', '%(t)s', %(f)s, %(p)s, %(c)s, %(unique)s, '%(query_uuid)s', %(aq)s, %(sq)s)"""%locals()
        if debug: print 'ManifoldQuery.to_json:',result
        return result
