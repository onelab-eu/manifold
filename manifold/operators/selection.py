from manifold.operators             import Node
from manifold.operators.projection  import Projection
from manifold.util.log              import Log

DUMPSTR_SELECTION  = "WHERE %s"

#------------------------------------------------------------------
# Selection node (WHERE)
#------------------------------------------------------------------

class Selection(Node):
    """
    Selection operator node (cf WHERE clause in SQL)
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, filters):
        """
        \brief Constructor
        \param child A Node instance (the child of this Node)
        \param filters A set of Predicate instances
        """
        assert issubclass(type(child), Node), "Invalid child = %r (%r)"   % (child,   type(child))
        assert isinstance(filters, set),      "Invalid filters = %r (%r)" % (filters, type(filters))

        super(Selection, self).__init__()

        self._filters = filters

#        old_cb = child.get_callback()
#        child.set_callback(self.child_callback)
#        self.set_callback(old_cb)
#
#        self.query = self.child.get_query().copy()
#        self.query.filters |= filters
    
    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------
    
    def __repr__(self):
        return DUMPSTR_SELECTION % ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self._filters])


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """

        if packet.get_type() == Packet.TYPE_QUERY:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()
            packet.update_query(Query.unfilter_by, self.predicate)
            self.send(new_packet)

        elif packet.get_type() == Packet.TYPE_RECORD:
            record = packet

            if record.is_last() or (self._filters and self._filters.match(record)):
                self.send(record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent = 0):
        """
        \brief Dump the current child
        \param indent The current indentation
        """
        Node.dump(self, indent)
        self.child.dump(indent + 1)

#    def optimize_selection(self, filter):
#        # Concatenate both selections...
#        for predicate in self._filters:
#            filter.add(predicate)
#        return self.child.optimize_selection(filter)
#
#    def optimize_projection(self, fields):
#        # Do we have to add fields for filtering, if so, we have to remove them after
#        # otherwise we can just swap operators
#        keys = self._filters.keys()
#        self.child = self.child.optimize_projection(fields | keys)
#        self.query.fields = fields
#        if not keys <= fields:
#            # XXX add projection that removed added_fields
#            # or add projection that removes fields
#            old_self_callback = self.get_callback()
#            projection = Projection(self, fields)
#            projection.set_callback(old_self_callback)
#            return projection
#        return self
