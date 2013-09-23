from manifold.operators       import Node, LAST_RECORD, ChildCallback, ChildStatus
from manifold.core.filter     import Filter
from manifold.util.predicate  import Predicate, eq
from manifold.util.type       import returns
from manifold.util.log        import Log
from itertools                import product, imap

DUMPSTR_CROSSPRODUCT      = "XPRODUCT"

#------------------------------------------------------------------
# CROSS PRODUCT node
#------------------------------------------------------------------

class CrossProduct(Node):
    """
    CROSS PRODUCT operator node
    """

    def __init__(self, children_ast_relation_list, query = None):
        """
        \brief Constructor
        Args:
            children_ast_relation_list: A list of pair made
                of a Query and a Relation instance
            query: A Query instance
        """
        # children_ast_relation_list example: [
        #     (
        #         SELECT * AT now FROM tdmi:destination,
        #         <LINK_11, destination>
        #     ),
        #     (
        #         SELECT * AT now FROM tdmi:agent,
        #         <LINK_11, agent>
        #     )
        # ]

        super(CrossProduct, self).__init__()

        assert len(children_ast_relation_list) >= 2, \
            "Cannot create a CrossProduct from %d table (2 or more required): %s" % (
                len(children_ast_relation_list), children_ast_relation_list
            )

        # Note we cannot guess the query object, so pass it
        # fields = [relation.get_relation_name(r) for r in relation]
        # NTOE: such a query should not contain any action
        self.query = query
        self.children, self.relations = [], []
        for _ast, _relation in children_ast_relation_list:
            self.children.append(_ast)
            self.relations.append(_relation)

        self.child_results = []
        self.status = ChildStatus(self.all_done)

        # Set up callbacks & prepare array for storing results from children:
        # parent result can only be propagated once all children have replied
        for i, child in enumerate(self.children):
            child.set_callback(ChildCallback(self, i))
            self.child_results.append([])

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        for child in self.children:
            child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_CROSSPRODUCT

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start all children
        for i, child in enumerate(self.children):
            self.status.started(i)
        for i, child in enumerate(self.children):
            child.start()

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by the child node
        \param child_id identifier of the child that received the record
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            self.status.completed(child_id)
            return
        # We could only add the information of interest here, instead of doing
        # it in all_done
        # p = self.relations[i].get_predicate()
        self.child_results[child_id].append(record)

    def all_done(self):
        """
        \brief Called when all children of the current cross product are done

        Example:
        SQ agents = [{'agent_id': X1, 'agent_dummy': Y1}, {'agent_id': X2, 'agent_dummy': Y2}]
            p = ('agent', eq, 'agent_id')
        SQ dests  = [{'dest_id' : X1, 'dest_dummy' : Y1}, {'dest_id' : X2, 'dest_dummy' : Y2}]
            p = ('dest', eq, 'dest_id')
        X = [{'agent': X1, 'dest' : X1}, {'agent': X1, {'dest' : X2}, ...]
        """

        keys = set()
        for relation in self.relations:
            keys |= relation.get_predicate().get_value_names()

        def merge(dics):
            return { k: v for dic in dics for k,v in dic.items() if k in keys }

        records = imap(lambda x: merge(x), product(*self.child_results))
        map(lambda x: self.send(x), records)
        self.send(LAST_RECORD)
        
    def optimize_selection(self, filter):
        for i, child in enumerate(self.children):
            child_fields = child.query.get_select()
            child_filter = Filter()
            for predicate in filter:
                if predicate.get_field_names() <= child_fields:
                    child_filter.add(predicate)
            if child_filter:
               self.children[i] = child.optimize_selection(child_filter) 
        return self

    def optimize_projection(self, fields):
        for i, child in enumerate(self.children):
            child_fields = child.query.get_select()
            if not child_fields <= fields:
               self.children[i] = child.optimize_projection(child_fields & fields)
        return self
