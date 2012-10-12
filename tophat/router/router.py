from tophat.router.conf import Conf
from tophat.router.rib import RIB
from tophat.router.fib import FIB
from tophat.router.flowtable import FlowTable

from sqlalchemy.sql import operators

import copy
import time
import random
import base64

from tophat.auth import Auth
from tophat.models import *
from tophat.util.misc import get_sqla_filters, xgetattr

class LocalRouter(object):
    """
    Implements an abstraction of a Router.
    """

    LOCAL_NAMESPACE = 'tophat'

    _map_local_table = {
        'platform': Platform,
        'user': User,
        'account': Account
    }

    def __init__(self, dest_cls=object, route_cls=object):
        self.route_cls = route_cls

        self.conf = Conf()
        self.rib = RIB(dest_cls, route_cls)
        self.fib = FIB(route_cls)
        self.flow_table = FlowTable(route_cls)

        self.boot()

        # account.manage()

        # XXX we insert a dummy platform
        #p = Platform(platform = 'mytestbed', platform_longname='MyTestbed')
        #db.add(p) 
        #p = Platform(platform = 'tophat', platform_longname='TopHat')
        #db.add(p) 


    def boot(self):
        print "I: Booting router"
        # Install static routes in the RIB and FIB (TODO)
        print "D: Reading static routes in: '%s'" % self.conf.STATIC_ROUTES_FILE
        static_routes = self.get_static_routes(self.conf.STATIC_ROUTES_FILE)
        for r in static_routes:
            pass
        #self.rib[dest] = route
        self.build_tables()

        # Read peers into the configuration file
        # TODO

    def authenticate(self, auth):
        return Auth(auth).check()

    def get_session(self, auth):
        # Before a new session is added, delete expired sessions
        db.query(Session).filter(Session.expires < int(time.time())).delete()

        s = Session()
        # Generate 32 random bytes
        bytes = random.sample(xrange(0, 256), 32)
        # Base64 encode their string representation
        s.session = base64.b64encode("".join(map(chr, bytes)))
        s.user = self.authenticate(auth)
        s.expires = int(time.time()) + (24 * 60 * 60)
        db.add(s)
        db.commit()
        return s.session

    def get_query_plane(self, packet):
        pass

    def get_route(self, packet):
        pass

    def do_forward(self, query, route, user=None):
        raise Exception, "Not implemented"

    def local_query_get(self, query):
        #
        # XXX How are we handling subqueries
        #

        fields = query.fields
        # XXX else tap into metadata

        cls = self._map_local_table[query.fact_table]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.fields) if query.fields else None

        if query.fields:
            res = db.query( *_fields ).filter(_filters)
        else:
            res = db.query( cls ).filter(_filters)

        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]
        table = [ dict([(fields[idx], val) for idx, val in enumerate(t)]) for t in tuplelist]
        return table

    def local_query_update(self, query):

        cls = self._map_local_table[query.fact_table]

        _fields = xgetattr(cls, query.fields)
        _filters = get_sqla_filters(cls, query.filters)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in query.params.items() ])

        db.query(cls).update(_params, synchronize_session=False)
        #db.query(cls).filter(_filters).update(_params, synchronize_session=False)
        db.commit()

        return []

    def local_query(self, query):
        _map_action = {
            'get': self.local_query_get,
            'update': self.local_query_update
        }
        return _map_action[query.action](query)

    
    # This function is directly called for a LocalRouter
    # Decoupling occurs before for queries received through sockets
    def forward(self, query, deferred=False, execute=True, user=None):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """

        # Handling internal queries
        if ':' in query.fact_table:
            #try:
            namespace, table = query.fact_table.rsplit(':', 2)
            if namespace == self.LOCAL_NAMESPACE:
                q = copy.deepcopy(query)
                q.fact_table = table
                return self.local_query(q)
            elif namespace == 'metadata':
                # Metadata are obtained for the 3nf representation in
                # memory
                if table == 'table':
                    output = []
                    # XXX Not generic
                    for table in self.G_nf.graph.nodes():
                        fields = [f for f in self.G_nf.get_fields(table)]
                        fields = list(set(fields))

                        # Build columns from fields
                        columns = []
                        for field in fields:
                            column = {
                                'column': field,
                                'description': field,
                                'header': field,
                                'title': field,
                                'unit': 'N/A',
                                'info_type': 'N/A',
                                'resource_type': 'N/A',
                                'value_type': 'N/A',
                                'allowed_values': 'N/A'
                            }
                            columns.append(column)

                        # Add table metadata
                        output.append({'table': table.name, 'column': columns})
                    return output
                else:
                    raise Exception, "Unsupported metadata request '%s'" % table
            else:
                raise Exception, "Unsupported namespace '%s'" % namespace
            #except Exception, e:
            #    raise Exception, "Error during local request: %s" % e
        route = None

        #print "(forward)"

        # eg. a query arrive (similar to a packet)packet arrives (query)
        
        # we look at the destination of the query
        # valid destinations are the ones that form a DAG given the NF schema
        #destination = query.destination
        #print "(got destination)", destination
        #
        # In flow table ?
        #try:
        #    print "(searching for route in flow table)"
        #    route = self.flow_table[destination]
        #    print "(found route in flow table)"
        #except KeyError, key:
        #    print "(route not in flow table, try somewhere else)"
        #    # In FIB ?
        #    try:
        #        route = self.fib[destination]
        #    except KeyError, key:
        #        # In RIB ? raise exception if not found
        #        try:
        #            route = self.rib[destination]
        #        except KeyError, key:
        #            raise Exception, "Unknown destination: %r" % key
        #            return None
        #
        #        # Add to FIB
        #        fib[destination] = route
        #    
        #    # Add to flow table
        #    flow_table[destination] = route

        return self.do_forward(query, route, deferred, execute, user)
            
        # in tophat this is a AST + a set of queries to _next_hops_
        #  - we forward processed subqueries to next hops and we process them
        #  - out = f(FW(f(in, peer1)), FW(in, peer2), FW(...), ...)
        #    This is an AST !!! we need to decouple gateways for ends of the query plane / AST
        #  - a function of what to do with the list of results : not query by query but result by result... partial combination also work...
        #  - in fact a multipipe in which ot insert any result that come
        # in BGP this is a next hop to which to forward
        #
        # if the destination is not in the FIB, compute the associated route and add it, otherwise retrieve it (the most specific one)
        # TODO Steiner tree, dmst, spf, etc.
        # Typically a BGP router maintains a shortest path to all destinations, we don't do this.
        #
        # Eventually pass the message to the data plane, to establish query plane (route + operators from query) and circuits (gateways)
        #
        # Are we waiting for an answer or not (one shot query, callback (different communication channel), changes (risk of timeout), streaming)

class FlowAwareRouter(LocalRouter):

    def __init__(self):
        self.flow_table = FlowTable()

    def get_query_plane(self, packet):

        # Get flow from packet

        try:
            query_plane = self.flowtable[flow]

        # TODO change exception name
        except KeyError, flow:
            # Compute query_plane from route in FIB
            query_plane = None

            try:
                route = self.fib[destination]

            except KeyError, destination:
            
                # Compute route from routes in RIB
                route = None

                # Insert route in FIB
                self.fib.add(route)
            
            # We have the route, compute a query plane
            query_plane = None

        
                        

class Router(LocalRouter):
    def boot(self):
        super(Router, self).boot()

        # Establish session towards peers for dynamic route update (if needed)
        # TODO

        # Listen for queries (if needed)
        # TODO
        
