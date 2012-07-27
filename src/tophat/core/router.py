from tophat.router import *

class ParameterError(StandardError): pass

class Table:
    """
    Implements a database table schema.
    """

    def __init__(self, name, fields, keys):
        self.name = name
        self.fields = fields
        self.keys = keys

class TableSet(set):
    def add(self, element):

        # Force destinations to be sets of tables
        if type(element) != Table:
            raise TypeError("Element of type %s expected in argument. Got %s." % (type(element), Table))

        super(TopHatDestination, self).add(element)


class THDestination(Destination):
    """
    Implements a destination in TopHat 

    = a fact table + a set of tables/fields.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.

    """
    
    def __init__(self, fact_table, filters, fields):
        self.fact_table = fact_table
        self.filters = filters
        self.fields = fields

class THQuery(Query):
    """
    Implements a TopHat query.
    """

    def __init__(self, *args, **kwargs):
        l = len(kwargs.keys())

        # range(x,y) <=> [x, y[
        if len(args) in range(1,4) and type(args) == tuple:
            self.destination = THDestination(*args)
            return
        elif 'destination' in kwargs:
            destination = kwargs['destination']
            if type(destination) != THDestination:
                raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), THDestination))
            self.destination = kwargs[destination]
            del kwargs['destination']
            
            if not kwargs: return 
        elif 'fact_table' in kwargs:
            fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                filters = kwargs['filters']
                del kwargs['filters']
            else:
                filters = None

            if 'fields' in kwargs:
                fields = kwargs['fields']
                del kwargs['fields']
            else:
                fields = None

            self.destination = THDestination(fact_table, filters, fields)

            if not kwargs: return
        
        raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
        
        super(THQuery, self).__init__(self, destination)

class THRoute(Route):
    """
    Implements a TopHat route.
    """

    def __init__(self, destination, peer, cost, timestamp):

        if type(destination) != THDestination:
            raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), THDestination))

        # Assert the route corresponds to an existing peer
        # Assert the cost corresponds to a right cost
        # Eventually the timestamp would not be a parameter but assigned

        super(THRoute, self).__init__(self, destination, peer, cost, timestamp)

class THCost(int):
    """
    Let's use (N, min,  +) semiring for cost
    """

    def __add__(self, other):
        return THCost(min(self, other))

    def __mul__(self, other):
        return THCost(self + other)

class THLocalRouter(LocalRouter):
    """
    Implements a TopHat router.

    Specialized to handle THAnnounces/THRoutes, ...
    """
    def get_platform_max_fields(self, fields):
        # Search for the platform::method that allows for the largest number of missing fields
        maxfields = 0
        ret = None
        methods = Metadata(self.api).get_methods()
        for m in methods:
            isect = set(m['fields']).intersection(set(fields))
            if len(isect) > maxfields:
                maxfields = len(isect)
                ret = m
        return ret

    def compute_route(self, query):

        from tophat.core.ast import AST
        from tophat.core.metadata import Metadata
        from tophat.core.gateway import Gateway

        dest = query.destination
        fact_table, filters, fields = dest.fact_table, dest.filters, dest.fields

        # This method is broken, need to replace it with steiner

        method = query.get_method() 
        fields.extend([x[0] for x in filters])
        fields = set(fields)
         
        # Query plan 
        qp = AST()
        join = False 

        # Note: We could skip or restrict the set of platforms, and ask for routing or timing information 
        while True: 
            p = self.get_platform_max_fields(fields) 
            if not p: 
                raise Exception, "Cannot complete query: %s" % fields 

            # HACK 
            print p['platform'] 
            if method == 'resources' and p['platform'] == 'tophat': 
                method = 'nodes' 

            q = Query(method, ts, {}, list(p['fields'])) 
            gateways = MetadataGateways(self.api, {'platform': p['platform']}) 
            if not gateways: 
                raise Exception, "No gateway found for platform '%(platform)s'" % p 
            config = json.loads(gateways[0]['config']) 

            # We add the caller to the config parameter 
            config['caller'] = self.caller 

            network = Gateway.factory(self.api, q, **config) 
            if not join: 
                qp = qp.From(network) 
                join = True 
            else: 
                r = AST().From(network) 
                # we join on hostname (hardcoded) 
                qp = qp.join(r, 'hostname') 
            for f in p['fields']: 
                if f in fields: 
                    fields.remove(f) 
            if not fields: 
                break 
        qp = qp.selection(query.get_filters()) 
        qp = qp.projection(query.get_fields()) 
        #qp = qp.sort(query.get_sort()) 
        #qp = qp.limit(query.get_limit()) 

        return qp
        #return list(qp._get()) 

        

class THRouter(THLocalRouter, Router):
    pass


