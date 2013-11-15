# -*- coding: utf-8 -*-

# We currently build on QueryPlan, the idea is in the end to merge the QueryPlan class in this class.
from manifold.core.query_plan import QueryPlan
from manifold.util.lattice    import Lattice

class OperatorGraph(object):
    """
    Replaces QueryPlan() and AST(), since operators are now mutualized.

    To begin with, the operator graph will be a set of parallel ASTs.
    """

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

    def build_query_plan(query, annotations, receiver):
        user = annotations.get('user', None)

        # XXX Code duplication 
        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table
            allowed_platforms = [p['platform'] for p in self._router.get_platforms() if p['platform'] == namespace]
        else:
            allowed_platforms = [p['platform'] for p in self._router.get_platforms()]

        query_plan = QueryPlan()
        query_plan.build(query, self._router.g_3nf, allowed_platforms, self._router.allowed_capabilities, user)
        #query_plan.dump()

        self._router.init_from_nodes(query_plan, user)
        #XXX#self.instanciate_gateways(query_plan, user) # removed by marco ????


        #return self.execute_query_plan(query, annotations, query_plan, is_deferred)
        query_plan.ast.set_callback(receiver.receive)
        query_plan.ast.start()
