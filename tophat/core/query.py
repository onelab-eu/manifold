from tophat.core.filter import Filter

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
        if len(args) in range(2,6) and type(args) == tuple:
            # Note: range(x,y) <=> [x, y[
            self.action, self.fact_table, self.filters, self.params, self.fields = args
            self.params = set(self.params)
            self.filters = Filter(self.filters)
            self.fields = set(self.fields)

        # Initialization from a dict (action & fact_table are mandatory)
        elif 'fact_table' in kwargs:
            if 'action' in kwargs:
                self.action = kwargs['action']
                del kwargs['action']
            else:
                self.action = 'get'

            self.fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                self.filters = kwargs['filters']
                del kwargs['filters']
            else:
                self.filters = None

            if 'fields' in kwargs:
                self.fields = set(kwargs['fields'])
                del kwargs['fields']
            else:
                self.fields = None

            if 'params' in kwargs:
                self.params = kwargs['params']
                del kwargs['params']
            else:
                self.params = None

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
                return
        else:
                raise ParameterError, "No valid constructor found for %s" % self.__class__.__name__

