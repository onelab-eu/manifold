from tophat.router.conf import Conf
from tophat.router.rib import RIB
from tophat.router.fib import FIB
from tophat.router.flowtable import FlowTable

class LocalRouter(object):
    """
    Implements an abstraction of a Router.
    """

    def __init__(self, dest_cls=object, route_cls=object):
        self.route_cls = route_cls

        self.conf = Conf()
        self.rib = RIB(dest_cls, route_cls)
        self.fib = FIB(route_cls)
        self.flow_table = FlowTable(route_cls)

        self.boot()

        
    def _get_static_routes(self):
        #print "D: Reading %s" % self.conf.STATIC_ROUTES_FILE
        self.get_static_routes(self.conf.STATIC_ROUTES_FILE)

    def boot(self):
        # Install static routes in the RIB and FIB (TODO)
        static_routes = self._get_static_routes()

        # Read peers into the configuration file
        # TODO

    def get_query_plane(self, packet):
        pass

    def get_route(self, packet):
        pass

    def do_forward(self, query, route):
        raise Exception, "Not implemented"
    
    # This function is directly called for a LocalRouter
    # Decoupling occurs before for queries received through sockets
    def forward(self, query, deferred=False):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """

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

        return self.do_forward(query, route, deferred)
            
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
        
