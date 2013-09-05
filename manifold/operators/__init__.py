import random, traceback
from types                import StringTypes
from manifold.core.query  import Query
from manifold.core.filter import Filter
from manifold.util.type   import returns
from manifold.util.log    import Log

# This constant is used by the gateways and the operators to signal the last
# record returned for a query
LAST_RECORD = None

#------------------------------------------------------------------
# Utility classes
#------------------------------------------------------------------

class ChildCallback:
    """
    Implements a child callback function able to store its identifier.
    """

    def __init__(self, parent, child_id):
        """
        Constructor
        \param parent Reference to the parent class
        \param child_id Identifier of the children
        """
        self.parent, self.child_id = parent, child_id

    def __call__(self, record):
        """
        \brief Process records received by the callback
        """
        # Pass the record to the parent with the right child identifier
        try:
            self.parent.child_callback(self.child_id, record)
        except Exception, e:
            Log.error("EXCEPTION IN ChildCallback, calling %s. %s" % (self.parent.child_callback, traceback.format_exc()))
            raise e

#------------------------------------------------------------------
# ChildStatus
#------------------------------------------------------------------

class ChildStatus:
    """
    Monitor child completion status
    """

    def __init__(self, all_done_cb):
        """
        \brief Constructor
        \param all_done_cb Callback to raise when are children are completed
        """
        self.all_done_cb = all_done_cb
        self.counter = 0

    def started(self, child_id):
        """
        \brief Call this function to signal that a child has completed
        \param child_id The integer identifying a given child node
        """
        self.counter += child_id + 1

    def completed(self, child_id):
        """
        \brief Call this function to signal that a child has completed
        \param child_id The integer identifying a given child node
        """
        self.counter -= child_id + 1
        assert self.counter >= 0, "Child status error: %d" % self.counter
        if self.counter == 0:
            try:
                self.all_done_cb()
            except Exception, e:
                print "EXCEPTION IN ChildStatus, calling", self.all_done_cb
                print e
                import traceback
                traceback.print_exc()

#------------------------------------------------------------------
# Node (parent class)
#------------------------------------------------------------------

class Node(object):
    """
    \brief Base class for implementing AST node objects
    """

    def __init__(self, node = None):
        """
        \brief Constructor
        """
        if node:
            if not isinstance(node, Node):
                raise ValueError('Expected type Node, got %s' % node.__class__.__name__)
            return deepcopy(node)
        # Callback triggered when the current node produces data.
        self.callback = None
        # Query representing the data produced by the node.
#        self.query = self.get_query()
        self.identifier = random.randint(0,9999)

    def set_callback(self, callback):
        """
        \brief Associates a callback to the current node
        \param callback The callback to associate
        """
        self.callback = callback
        # Basic information about the data provided by a node

    def get_callback(self):
        """
        \brief Return the callback related to this Node 
        """
        return self.callback

    def send(self, record):
        """
        \brief calls the parent callback with the record passed in parameter
        """
        Log.record("[#%04d] SEND %r [ %r ]" % (self.identifier, self.__class__.__name__, record))
        self.callback(record)

    @returns(Query)
    def get_query(self):
        """
        \brief Returns the query representing the data produced by the nodes.
        \return query representing the data produced by the nodes.
        """

        return self.query

        # Raise an exception in the base class to force child classes to
        # implement this method
        #raise Exception, "Nodes should implement the query function: %s" % self.__class__.__name__

    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        print "[%04d]" % self.identifier, ' ' * 4 * indent,
        #        sys.stdout.write(' ' * indent * 4)

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self
        #print "%r (%r)" % (self, self.query)
        #print "%r (%r)" % (self, self.callback)

    @returns(StringTypes)
    def __repr__(self):
        return "This method should be overloaded!"

    @returns(StringTypes)
    def __str__(self):
        return self.__repr__() 

    def optimize(self):
        Log.warning("Calling optimize()")
        tree = self.optimize_selection(Filter())
        tree = tree.optimize_projection(set())
        return tree
    
    def optimize_selection(self, filter):
        Log.error("%s::optimize_selection() not implemented" % self.__class__.__name__)

    def optimize_projection(self, fields):
        #raise Exception, "%s::optimize_projection() not implemented" % self.__class__.__name__
        Log.error("%s::optimize_projection() not implemented" % self.__class__.__name__)

    def get_identifier(self):
        return self.identifier
