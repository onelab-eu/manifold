#
# TODO: add timestamp field
#

from types import StringTypes
from tophat.core.filter import Filter, Predicate
from tophat.util.frozendict import frozendict

class ParameterError(StandardError): pass

class Query(object):
    """
    Implements a TopHat query.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.
    """

    def __init__(self, *args, **kwargs):
        l = len(kwargs.keys())

        # Initialization from a tuple
        if len(args) in range(2, 6) and type(args) == tuple:
            # Note: range(x,y) <=> [x, y[
            self.action, self.fact_table, self.filters, self.params, self.fields = args


        # Initialization from a dict (action & fact_table are mandatory)
        elif 'fact_table' in kwargs:
            if 'action' in kwargs:
                self.action = kwargs['action']
                del kwargs['action']
            else:
                print "W: defaulting to get action"
                self.action = 'get'

            self.fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                self.filters = kwargs['filters']
                del kwargs['filters']
            else:
                self.filters = Filter([])

            if 'fields' in kwargs:
                self.fields = set(kwargs['fields'])
                del kwargs['fields']
            else:
                self.fields = set([])

            if 'params' in kwargs:
                self.params = kwargs['params']
                del kwargs['params']
            else:
                self.params = {}

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
                return
        else:
                raise ParameterError, "No valid constructor found for %s : args=%r" % (self.__class__.__name__, args)

        if not self.filters: self.filters = Filter([])
        if not self.params:  self.params  = {}
        if not self.fields:  self.fields  = set([])

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

    def __str__(self):
        return "SELECT %s FROM %s WHERE %s" % (', '.join(self.fields), self.fact_table, self.filters)

    def __key(self):
        return (self.action, self.fact_table, self.filters, frozendict(self.params), frozenset(self.fields))

    def __hash__(self):
        return hash(self.__key())

