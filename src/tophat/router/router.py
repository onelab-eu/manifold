from tophat.router.conf import Conf
from tophat.router.rib import RIB
from tophat.router.fib import FIB

class LocalRouter:
    """
    Implements an abstraction of a Router.
    """

    def __init__(self, announce_cls=object):
        self.announce_cls = announce_cls

        self.conf = Conf()
        self.rib = RIB(announce_cls)
        self.fib = FIB(announce_cls)

        self.boot()

        
    def get_static_routes(self):
        print "D: Reading %s" % self.conf.STATIC_ROUTES_FILE

    def boot(self):
        # Install static routes in the RIB and FIB (TODO)
        static_routes = self.get_static_routes()

        # Read peers into the configuration file
        # TODO

    def get_query_plane(self, packet):
        pass

    def get_route(self, packet):
        pass
    
    def forward(self, query):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """

        # eg. a query arrive (similar to a packet)packet arrives (query)
        
        # we look at the destination of the query
        # valid destinations are the ones that form a DAG given the NF schema
        destination = query.destination

        route = self.get_route(destination)

        # if the destination is not in the FIB, compute the associated route and add it, otherwise retrieve it (the most specific one)
        # TODO Steiner tree, dmst, spf, etc.
        # Typically a BGP router maintains a shortest path to all destinations, we don't do this.
        route = None

        # Eventually pass the message to the data plane, to establish query plane (route + operators from query) and circuits (gateways)

        # Are we waiting for an answer or not (one shot query, callback (different communication channel), changes (risk of timeout), streaming)
        return True

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
        
