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

import copy, json, uuid, traceback

from types                          import StringTypes
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter, Predicate
from manifold.core.fields           import Fields
from manifold.util.clause           import Clause
from manifold.util.frozendict       import frozendict
from manifold.util.log              import Log
from manifold.util.misc             import is_iterable
from manifold.util.type             import returns, accepts

ACTION_NONE    = ''
ACTION_CREATE  = 'create'
ACTION_GET     = 'get'
ACTION_UPDATE  = 'update'
ACTION_DELETE  = 'delete'
ACTION_EXECUTE = 'execute'

def uniqid():
    return uuid.uuid4().hex

class ParameterError(StandardError): pass

class Query(object):
    """
    Implements a Manifold Query.

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
                self.action = ACTION_GET
                self.params = {}
                self.timestamp     = 'now'
                self.object, self.filters, self.fields = args
            elif len_args == 4:
                self.object, self.filters, self.params, self.fields = args
                self.action = ACTION_GET
                self.timestamp     = 'now'
            else:
                self.action, self.object, self.filters, self.params, self.fields, self.timestamp = args

        # Initialization from a dict
        elif "object" in kwargs:
            if "action" in kwargs:
                self.action = kwargs["action"]
                del kwargs["action"]
            else:
                Log.warning("query: Defaulting to get action")
                self.action = ACTION_GET


            self.object = kwargs["object"]
            del kwargs["object"]

            if "filters" in kwargs:
                self.filters = kwargs["filters"]
                del kwargs["filters"]
            else:
                self.filters = Filter()

            if "fields" in kwargs:
                # '*' Handling
                fields = kwargs.pop('fields')
                if '*' in fields:
                    #Log.tmp("SET STAR IN KWARGS")
                    self.fields = Fields(star = True)
                else:
                    self.fields = Fields(fields, star = False)
            else:
                self.fields = Fields(star = True)

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

            # Indicates the key used to store the result of this query in manifold-shell
            # Ex: $myVariable = SELECT foo FROM bar
            if "variable" in kwargs:
                self.variable = kwargs["variable"][0]
                del kwargs["variable"]
            else:
                self.variable = None

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
        #else:
        #        raise ParameterError, "No valid constructor found for %s : args = %r" % (self.__class__.__name__, args)

        self.sanitize()

    def sanitize(self):
        if not self.filters:   self.filters   = Filter()
        if not self.params:    self.params    = {}
        if not self.fields:    self.fields    = Fields()
        if not self.timestamp: self.timestamp = "now"

        if isinstance(self.filters, list):
            f = self.filters
            self.filters = Filter()
            for x in f:
                pred = Predicate(x)
                self.filters.add(pred)
        elif isinstance(self.filters, Clause):
            self.filters = Filter.from_clause(self.filters)

        if not isinstance(self.fields, Fields):
            self.fields = Fields(self.fields)

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def copy(self):
        return copy.deepcopy(self)
    clone = copy

    def clear(self):
        self.action = ACTION_GET
        self.object = None
        self.filters = Filter()
        self.params  = {}
        self.fields  = Fields()
        self.timestamp  = 'now' # ignored for now

    #@returns(StringTypes)
    def to_sql(self, platform = "", multiline = False):
        """
        Args:
            platform: A String corresponding to a namespace (or platform name)
            multiline: A boolean indicating whether the String could contain
                carriage return.
        Returns:
            The String representing this Query.
        """
        get_params_str = lambda : ", ".join(["%s = %r" % (k, v) for k, v in self.get_params().items()])

        table  = self.get_from() # it may be a string like "namespace:table_name" or not
        fields = self.get_fields()
        select = "SELECT %s" % ("*" if fields.is_star() else ", ".join([field for field in fields]))
        where  = "WHERE %s"  % self.get_where()     if self.get_where()     else ""
        at     = "AT %s"     % self.get_timestamp() if self.get_timestamp() else ""
        params = "SET %s"    % get_params_str()     if self.get_params()    else ""

        sep = " " if not multiline else "\n  "
        platform = "%s:" % platform if platform else ""

        strmap = {
            ACTION_GET    : "%(select)s%(sep)s%(at)s%(sep)sFROM %(platform)s%(table)s%(sep)s%(where)s",
            ACTION_UPDATE : "UPDATE %(platform)s%(table)s%(sep)s%(params)s%(sep)s%(where)s%(sep)s%(select)s",
            ACTION_CREATE : "INSERT INTO %(platform)s%(table)s%(sep)s%(params)s",
            ACTION_DELETE : "DELETE FROM %(platform)s%(table)s%(sep)s%(where)s"
        }

        #Log.tmp(strmap[self.action] % locals())
        return strmap[self.action] % locals()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Query.
        """
        return self.to_sql() #multiline=True)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Query.
        """
        return self.to_sql()

    #---------------------------------------------------------------------------
    # Conversion
    #---------------------------------------------------------------------------

    @returns(dict)
    def to_dict(self):
        return {
            "action"    : self.get_action(),
            "object"    : self.get_from(),
            "timestamp" : self.get_timestamp(),
            "filters"   : self.get_where().to_list(),
            "params"    : self.get_params(),
            "fields"    : list(self.get_select())
        }

    @returns(StringTypes)
    def to_json(self, analyzed_query=None):
        query_uuid=self.query_uuid
        a=self.action
        o=self.object
        t=self.timestamp
        f=json.dumps (self.filters.to_list())
        p=json.dumps (self.params)
        c=json.dumps (list(self.fields)) if not self.fields.is_star() else 'null'
        # xxx unique can be removed, but for now we pad the js structure
        unique=0

        if not analyzed_query:
            aq = 'null'
        else:
            aq = analyzed_query.to_json()
        sq="{}"

        result= """ new ManifoldQuery('%(a)s', '%(o)s', '%(t)s', %(f)s, %(p)s, %(c)s, %(unique)s, '%(query_uuid)s', %(aq)s, %(sq)s)"""%locals()
        if debug: Log.info('ManifoldQuery.to_json:', result)
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
            Log.warning("Could not decode incoming ajax request as a Query, POST= %s" % POST_dict)
            Log.debug(traceback.print_exc())
        self.sanitize()

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def get_variable(self):
        return self.variable

    @returns(StringTypes)
    def get_action(self):
        """
        Returns:
            The String related to this Query instance.
            It's a value among ACTION_CREATE, ACTION_GET, ACTION_UPDATE,
            ACTION_DELETE, ACTION_EXECUTE.
        """
        return self.action

    def set_action(self, action):
        """
        Set the action related to this Query instance.
        Args:
            action: A String among ACTION_CREATE, ACTION_GET, ACTION_UPDATE,
            ACTION_DELETE, ACTION_EXECUTE.
        """
        self.action = action
        return self

    @returns(Fields)
    def get_select(self): # DEPRECATED
        return self.get_fields()

    @returns(Fields)
    def get_fields(self):
        return self.fields # frozenset(self.fields) if self.fields is not None else None

    @returns(StringTypes)
    def get_from(self): # DEPRECATED
        """
        Extracts the FROM clause of this Query.
        You should use get_table_name() or get_namespace().
        Returns:
            A String "namespace:table_name" or "table_name" (if the
            namespace is unset), where 'namespace' is the result of
            self.get_namespace() and 'table_name' is the result of
            self.get_table_name()
        """
        return self.get_object()

    @returns(StringTypes)
    def get_object(self):
        """
        Extracts the FROM clause of this Query.
        You should use get_table_name() or get_namespace().
        Returns:
            A String "namespace:table_name" or "table_name" (if the
            namespace is unset), where 'namespace' is the result of
            self.get_namespace() and 'table_name' is the result of
            self.get_table_name()
        """
        return self.object

    def set_object(self, object):
        self.object = object
        return self

    @returns(Filter)
    def get_where(self): # DEPRECATED
        return self.get_filter()

    @returns(Filter)
    def get_filter(self):
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

    @staticmethod
    #@returns(Query)
    def action(action, object):
        """
        (Internal usage). Craft a Query according to an action name
        See methods: get, update, delete, execute.
        Args:
            action: A String among ACTION_CREATE, ACTION_GET, ACTION_UPDATE,
            ACTION_DELETE, ACTION_EXECUTE.
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        query = Query()
        query.action = action
        query.object = object
        return query

    @staticmethod
    #@returns(Query)
    def get(object):
        """
        Craft the Query which fetches the records related to a given object
        Args:
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        return Query.action(ACTION_GET, object)

    @staticmethod
    #@returns(Query)
    def update(object):
        """
        Craft the Query which updates the records related to a given object
        Args:
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        return Query.action(ACTION_UPDATE, object)

    @staticmethod
    #@returns(Query)
    def create(object):
        """
        Craft the Query which create the records related to a given object
        Args:
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        return Query.action(ACTION_CREATE, object)

    @staticmethod
    #@returns(Query)
    def delete(object):
        """
        Craft the Query which delete the records related to a given object
        Args:
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        return Query.action(ACTION_DELETE, object)

    @staticmethod
    #@returns(Query)
    def execute(object):
        """
        Craft the Query which execute a processing related to a given object
        Args:
            object: The name of the queried object (String)
        Returns:
            The corresponding Query instance
        """
        return Query.action(ACTION_EXECUTE, object)

    #@returns(Query)
    def at(self, timestamp):
        """
        Set the timestamp carried by the query
        Args:
            timestamp: The timestamp (it may be a python timestamp, a string
                respecting the "%Y-%m-%d %H:%M:%S" python format, or "now")
        Returns:
            The self Query instance
        """
        self.timestamp = timestamp
        return self

    def filter_by(self, *args, **kwargs):
        """
        Args:
            args: It may be:
                - the parts of a Predicate (key, op, value)
                - None
                - a Filter instance
                - a set/list/tuple of Predicate instances
        """

        clear = kwargs.get('clear', False)
        # We might raise an Exception for other attributes
        if clear:
            self.filters = Filter()

        if len(args) == 1:
            filters = args[0]
            if filters == None:
                self.filters = Filter()
                return self
            if not isinstance(filters, (set, list, tuple, Filter)):
                filters = [filters]
            for predicate in filters:
                self.filters.add(predicate)
        elif len(args) == 3:
            predicate = Predicate(*args)
            self.filters.add(predicate)
        else:
            raise Exception, 'Invalid expression for filter'

        assert isinstance(self.filters, Filter),\
            "Invalid self.filters = %s" % (self.filters, type(self.filters))
        return self

    def unfilter_by(self, *args):
        if len(args) == 1:
            filters = args[0]
            if filters == None:
                return self
            if not isinstance(filters, (set, list, tuple, Filter)):
                filters = [filters]
            for predicate in set(filters):
                self.filters.remove(predicate)
        elif len(args) == 3:
            predicate = Predicate(*args)
            self.filters.remove(predicate)
        else:
            raise Exception, 'Invalid expression for filter'

        assert isinstance(self.filters, Filter),\
            "Invalid self.filters = %s" % (self.filters, type(self.filters))
        return self

    #@returns(Query)
    def select(self, *fields, **kwargs):
        """
        Update the SELECT clause of this Query.
        Args:
            fields: A list of String, where each String correspond to a Field name.
        Returns:
            The updated Query instance.
        """
        clear = kwargs.get('clear', False)
        # We might raise an Exception for other attributes

        # fields is a tuple of arguments
        if len(fields) == 1:
            tmp, = fields
            if tmp is None:
                # None = '*'
                self.fields = Fields(star = True)
            else:
                fields = Fields(tmp) if is_iterable(tmp) else Fields([tmp])
                if clear:
                    self.fields = fields
                else:
                    self.fields |= fields
            return self

        # We have an sequence of fields
        if clear:
            self.fields = Fields(star = False)
        for field in fields:
            self.fields.add(field)
        return self

    def set(self, params, clear = False):
        if clear:
            self.params = dict()
        self.params.update(params)
        return self

    def __or__(self, query):
        assert self.action == query.action
        assert self.object == query.object
        assert self.timestamp == query.timestamp # XXX
        filter = self.filters | query.filters
        # fast dict union
        # http://my.safaribooksonline.com/book/programming/python/0596007973/python-shortcuts/pythoncook2-chp-4-sect-17
        params = dict(self.params, **query.params)
        fields = self.fields | query.fields
        return Query.action(self.action, self.object).filter_by(filter).select(fields)

    def __and__(self, query):
        assert self.action == query.action
        assert self.object == query.object
        assert self.timestamp == query.timestamp # XXX
        filter = self.filters & query.filters
        # fast dict intersection
        # http://my.safaribooksonline.com/book/programming/python/0596007973/python-shortcuts/pythoncook2-chp-4-sect-17
        params =  dict.fromkeys([x for x in self.params if x in query.params])
        fields = self.fields & query.fields
        return Query.action(self.action, self.object).filter_by(filter).select(fields)

    def __eq__(self, other):
        return self.action == other.action and \
                self.object == other.object and \
                self.timestamp == other.timestamp and \
                self.filters == other.filters and \
                self.params == other.params and \
                self.fields == other.fields

    def __le__(self, other):
        return self.action == other.action and \
                self.timestamp == other.timestamp and \
                self.filters <= other.filters and \
                self.params == other.params and \
                self.fields <= other.fields

    # Defined with respect of previous functions

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return other <= self

    def __gt__(self, other):
        return other < self

    def __key(self):
        return (self.get_action(), self.get_from(), self.get_where(), frozendict(self.get_params()), frozenset(self.get_select()))

    def __hash__(self):
        #return hash(self.action) ^ \
        #    hash(self.object) ^ \
        #    hash(self.timestamp) ^ \
        #    hash(self.filters) ^ \
        #        self.params == other.params and \
        #        self.fields == other.fields
        return hash(self.__key())


    @returns(tuple)
    def get_namespace_table(self):
        """
        Retrieve the namespace and the table name related to
        this Query.
        Returns:
            A (String, String) tuple containing respectively
            the namespace and the table name of the FROM clause.
            If the namespace is unset, the first operand is
            set to None.
        """
        l = self.get_from().split(':', 1)
        return tuple(l) if len(l) == 2 else (None,) + tuple(l)

    def set_namespace_table(self, namespace, table_name):
        """
        Set the namespace and the table name of this Query.
        Args:
            namespace: A String instance.
            table_name: A String instance.
        """
        self.object = "%s:%s" % (namespace, table_name)

    @returns(StringTypes)
    def get_table_name(self):
        """
        Returns:
            The namespace corresponding to this Query (None if unset).
        """
        l = self.get_from().split(':', 1)
        return l[0] if len(l) == 2 else None


    @returns(StringTypes)
    def get_namespace(self):
        """
        Returns:
            The namespace corresponding to this Query (None if unset).
        """
        l = self.get_from().split(':', 1)
        return l[0] if len(l) == 2 else None

    @returns(StringTypes)
    def get_table_name(self):
        """
        Returns:
            The table_name corresponding to this Query (None if unset).
        """
        return self.get_from().split(':')[-1]

    def set_namespace(self, namespace):
        """
        Set the namespace and the table name of this Query.
        Args:
            namespace: A String instance.
        """
        old_namespace, old_table_name = self.get_namespace_table()
        self.set_namespace_table(namespace, old_table_name)

    @returns(StringTypes)
    def clear_namespace(self):
        """
        Unset the namespace set to this Query.
        """
        if self.get_namespace():
            self.object = self.get_table_name()

    #---------------------------------------------------------------------------
    # Destination
    #---------------------------------------------------------------------------

    @returns(Destination)
    def get_destination(self):
        """
        Craft the Destination corresponding to this Query.
        Returns:
            The corresponding Destination.
        """
        return Destination(self.object, self.filters, self.fields | Fields(self.params.keys()))

    def set_destination(self, destination):
        Log.warning("set_destination is not handling params")
        self.object  = destination.get_object()
        self.filters = destination.get_filter()
        self.fields  = destination.get_fields()
