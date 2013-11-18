# -*- coding: utf-8 -*-

# We currently build on QueryPlan, the idea is in the end to merge the QueryPlan class in this class.
from manifold.core.query_plan import QueryPlan
from manifold.util.lattice    import Lattice

class OperatorGraph(object):
    """
    Replaces QueryPlan() and AST(), since operators are now mutualized.

    To begin with, the operator graph will be a set of parallel ASTs.
    """

    # Duplicated in manifold.core.interface
    LOCAL_NAMESPACE = "local"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router):
        """
        Constructor
        """

        # A pointer to the router to which the OperatorGraph belongs
        self._router  = router
        
        # A lattice that maps the queries currently contained in the
        # OperatorGraph with the corresponding operators
        self._lattice = Lattice()


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def build_query_plan(self, packet):
        
        query      = packet.get_query()
        annotation = packet.get_annotation()
        receiver   = packet.get_receiver()

        user = annotation.get('user', None)

        # Handling platforms
        namespace = None

        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table

            if namespace == self.LOCAL_NAMESPACE:
                metadata = self._router.get_local_metadata()
                allowed_platforms = []

            else: # namespace == 1 platform
                metadata = self._router.get_metadata() #self._router.g_3nf
                allowed_platforms = [p['platform'] for p in self._router.get_platforms() if p['platform'] == namespace]
        else:
            metadata = self._router.g_3nf
            allowed_platforms = [p['platform'] for p in self._router.get_platforms()]

        # Handling metadata

        query_plan = QueryPlan()
        query_plan.build(query, metadata, allowed_platforms, self._router.allowed_capabilities, user)
        query_plan.dump()

        self._router.init_from_nodes(query_plan, user)
        #XXX#self.instanciate_gateways(query_plan, user) # removed by marco ????


        #return self.execute_query_plan(query, annotation, query_plan, is_deferred)
        query_plan.ast.set_callback(receiver.receive)

        root = query_plan.ast.get_root()
        print "ROOT=", root

        root.receive(packet)
