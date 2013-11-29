import random
from types                import StringTypes
from manifold.core.query  import Query
from manifold.core.filter import Filter
from manifold.util.type   import returns
from manifold.util.log    import Log

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
        Args:
            parent: Reference to the parent class
            child_id: Identifier of the children (integer)
        """
        self.parent, self.child_id = parent, child_id

    def __call__(self, record):
        """
        Process records received by the callback
        """
        # Pass the record to the parent with the right child identifier
        try:
            self.parent.child_callback(self.child_id, record)
        except Exception, e:
            Log.warning("EXCEPTION IN ChildCallback, calling %s" % self.parent.child_callback)
            Log.warning(e)
            import traceback
            traceback.print_exc()

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

# see code/node.py
#OBSOLETE|#------------------------------------------------------------------
#OBSOLETE|# Node (parent class)
#OBSOLETE|#------------------------------------------------------------------
#OBSOLETE|
#OBSOLETE|class Node(object):
#OBSOLETE|    """
#OBSOLETE|    \brief Base class for implementing AST node objects
#OBSOLETE|    """
#OBSOLETE|
#OBSOLETE|    def __init__(self, node = None):
#OBSOLETE|        """
#OBSOLETE|        \brief Constructor
#OBSOLETE|        """
#OBSOLETE|        if node:
#OBSOLETE|            if not isinstance(node, Node):
#OBSOLETE|                raise ValueError('Expected type Node, got %s' % node.__class__.__name__)
#OBSOLETE|            return deepcopy(node)
#OBSOLETE|        # Callback triggered when the current node produces data.
#OBSOLETE|        self.callback = None
#OBSOLETE|        # Query representing the data produced by the node.
#OBSOLETE|#        self.query = self.get_query()
#OBSOLETE|        self.identifier = random.randint(0,9999)
#OBSOLETE|
#OBSOLETE|    def set_callback(self, callback):
#OBSOLETE|        """
#OBSOLETE|        \brief Associates a callback to the current node
#OBSOLETE|        \param callback The callback to associate
#OBSOLETE|        """
#OBSOLETE|        self.callback = callback
#OBSOLETE|        # Basic information about the data provided by a node
#OBSOLETE|
#OBSOLETE|    def get_callback(self):
#OBSOLETE|        """
#OBSOLETE|        \brief Return the callback related to this Node 
#OBSOLETE|        """
#OBSOLETE|        return self.callback
#OBSOLETE|
#OBSOLETE|    def send(self, record):
#OBSOLETE|        """
#OBSOLETE|        \brief calls the parent callback with the record passed in parameter
#OBSOLETE|        """
#OBSOLETE|        Log.record(record, source=self)
#OBSOLETE|        self.callback(record)
#OBSOLETE|
#OBSOLETE|    @returns(Query)
#OBSOLETE|    def get_query(self):
#OBSOLETE|        """
#OBSOLETE|        \brief Returns the query representing the data produced by the nodes.
#OBSOLETE|        \return query representing the data produced by the nodes.
#OBSOLETE|        """
#OBSOLETE|
#OBSOLETE|        return self.query
#OBSOLETE|
#OBSOLETE|        # Raise an exception in the base class to force child classes to
#OBSOLETE|        # implement this method
#OBSOLETE|        #raise Exception, "Nodes should implement the query function: %s" % self.__class__.__name__
#OBSOLETE|
#OBSOLETE|    def tab(self, indent):
#OBSOLETE|        """
#OBSOLETE|        \brief print _indent_ tabs
#OBSOLETE|        """
#OBSOLETE|        print "[%04d]" % self.get_identifier(), ' ' * 4 * indent,
#OBSOLETE|        #        sys.stdout.write(' ' * indent * 4)
#OBSOLETE|
#OBSOLETE|    def dump(self, indent = 0):
#OBSOLETE|        """
#OBSOLETE|        \brief Dump the current node
#OBSOLETE|        \param indent current indentation
#OBSOLETE|        """
#OBSOLETE|        self.tab(indent)
#OBSOLETE|        print "%r" % self
#OBSOLETE|        #print "%r (%r)" % (self, self.query)
#OBSOLETE|        #print "%r (%r)" % (self, self.callback)
#OBSOLETE|
#OBSOLETE|    @returns(StringTypes)
#OBSOLETE|    def __repr__(self):
#OBSOLETE|        return "This method should be overloaded!"
#OBSOLETE|
#OBSOLETE|    @returns(StringTypes)
#OBSOLETE|    def __str__(self):
#OBSOLETE|        return self.__repr__() 
#OBSOLETE|
#OBSOLETE|    def optimize(self):
#OBSOLETE|        Log.info("Calling optimize()")
#OBSOLETE|        tree = self.optimize_selection(Filter())
#OBSOLETE|        tree = tree.optimize_projection(set())
#OBSOLETE|        return tree
#OBSOLETE|    
#OBSOLETE|    #@returns(Node)
#OBSOLETE|    def optimize_selection(self, filter):
#OBSOLETE|        Log.warning("optimize_selection() not implemented in" % self.__class__.__name__)
#OBSOLETE|        return self
#OBSOLETE|
#OBSOLETE|    #@returns(Node)
#OBSOLETE|    def optimize_projection(self, fields):
#OBSOLETE|        Log.warning("optimize_projection() not implemented in" % self.__class__.__name__)
#OBSOLETE|        return self
#OBSOLETE|
#OBSOLETE|    @returns(int)
#OBSOLETE|    def get_identifier(self):
#OBSOLETE|        return self.identifier
