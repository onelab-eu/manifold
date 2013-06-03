from manifold.core.query import Query
from manifold.operators  import Node, ChildCallback, LAST_RECORD
from manifold.util.type  import returns

#------------------------------------------------------------------
# DUP node
#------------------------------------------------------------------

class Dup(Node):

    def __init__(self, child, key):
        """
        \brief Constructor
        \param child A Node instance, child of this Dup Node
        \param key A Key instance
        """
        #assert issubclass(Node, type(child)), "Invalid child %r (%r)" % (child, type(child))
        #assert isinstance(Key,  type(key)),   "Invalid key %r (%r)"   % (key,   type(key))

        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        self.child_results = set()
        super(Dup, self).__init__()

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """
        return Query(self.child.get_query())

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "DUP (built above %r)" % self.get_child()
        self.get_child().dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()
        self.status.started(0)

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by a child node
        \param record dictionary representing the received record
        """
        assert child_id == 0

        if record == LAST_RECORD:
            self.status.completed(child_id)
            return

        if record not in self.child_results:
            self.child_results.add(record)
            self.send(record)
            return
