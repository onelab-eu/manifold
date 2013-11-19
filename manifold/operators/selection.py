from manifold.core.node             import Node
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.util.log              import Log

DUMPSTR_SELECTION  = "WHERE %s"

#------------------------------------------------------------------
# Selection node (WHERE)
#------------------------------------------------------------------

class Selection(Operator):
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

        Operator.__init__(self, producers = child, max_producers = 1)
        # XXX Shall we connect when passed as an argument
        self._filter = filters
        self.set_producer(child)


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
        print "filter====", self._filter
        return DUMPSTR_SELECTION % ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self._filter])


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """

        if packet.get_type() == Packet.TYPE_QUERY:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()
            packet.update_query(Query.unfilter_by, self._filter)
            self.send(new_packet)

        elif packet.get_type() == Packet.TYPE_RECORD:
            record = packet

            if record.is_last() or (self._filter and self._filter.match(record)):
                self.send(record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent = 0):
        """
        \brief Dump the current child
        \param indent The current indentation
        """
        Node.dump(self, indent)
        # We have one producer for sure
        self.get_producer().dump(indent + 1)

    def optimize_selection(self, query, filter):
        # Concatenate both selections...
        for predicate in self._filter:
            filter.add(predicate)
        return self.get_producer().optimize_selection(query, filter)

    def optimize_projection(self, query, fields):
        # Do we have to add fields for filtering, if so, we have to remove them after
        # otherwise we can just swap operators
        keys = self._filter.keys()
        self.update_producer(lambda p: p.optimize_projection(query, fields | keys))
        #self.query.fields = fields
        if not keys <= fields:
            # XXX add projection that removed added_fields
            # or add projection that removes fields
            return Projection(self, fields)
        return self
