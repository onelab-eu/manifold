from types               import StringTypes
from manifold.operators  import Node, ChildStatus, ChildCallback
from manifold.core.query import Query
from manifold.operators  import Node
from manifold.util.type  import returns

#------------------------------------------------------------------
# DEMUX node
#------------------------------------------------------------------

class Demux(Node):

    def __init__(self, child):
        """
        \brief Constructor
        \param child A Node instance, child of this Dup Node
        """
        super(Demux, self).__init__()
        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        self.query = self.child.get_query().copy()

    def add_callback(self, callback):
        """
        \brief Add a parent callback to this Node
        """
        self.parent_callbacks.append(callback)

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        for callback in self.parent_callbacks:
            callbacks(record)

    @returns(StringTypes)
    def __repr__(self):
        return "DEMUX (built above %r)" % self.get_child() 

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        self.get_child().dump(indent + 1)

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()
        self.status.started(0)

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def add_parent(self, parent):
        """
        \brief Add a parent Node to this Demux Node.
        \param parent A Node instance
        """
        assert issubclass(Node, type(parent)), "Invalid parent %r (%r)" % (parent, type(parent))
        print "not yet implemented"

    def optimize_selection(self, filter):
        self.child = self.child.optimize_selection(filter)
        return self

    def optimize_projection(self, fields):
        # We only need the intersection of both
        self.child = self.child.optimize_projection(fields)
        return self.child
