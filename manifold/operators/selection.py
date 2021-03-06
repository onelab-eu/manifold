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

    def __init__(self, child, filters):
        """
        \brief Constructor
        \param child A Node instance (the child of this Node)
        \param filters A set of Predicate instances
        """
        assert issubclass(type(child), Node), "Invalid child = %r (%r)"   % (child,   type(child))
        assert isinstance(filters, set),      "Invalid filters = %r (%r)" % (filters, type(filters))

        super(Selection, self).__init__()

        self.child, self.filters = child, filters

        old_cb = child.get_callback()
        child.set_callback(self.child_callback)
        self.set_callback(old_cb)

        self.query = self.child.get_query().copy()
        self.query.filters |= filters


#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the childs.
#        \return query representing the data produced by the childs.
#        """
#        print "Selection::get_query()"
#        q = Query(self.child.get_query())
#
#        # Selection add filters (union)
#        q.filters |= filters
#        return q

    def dump(self, indent = 0):
        """
        \brief Dump the current child
        \param indent The current indentation
        """
        return "%s\n%s" % (
            Node.dump(self, indent),
            self.child.dump(indent + 1),
        )

    def __repr__(self):
        return DUMPSTR_SELECTION % ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self.filters])

    def start(self):
        """
        \brief Propagates a START message through the child
        """
        self.child.start()

    def inject_insert(self, params):
        self.child.inject_insert(params)

    #@returns(Selection)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the child
        \param records A list of dictionaries representing records,
                       or list of record keys
        \return This node
        """
        self.child = self.child.inject(records, key, query) # XXX
        return self

    def child_callback(self, record):
        """
        \brief Processes records received by the child node 
        \param record dictionary representing the received record
        """

        if record.is_last() or (self.filters and self.filters.match(record)):
            self.send(record)

    def optimize_selection(self, filter):
        # Concatenate both selections...
        for predicate in self.filters:
            filter.add(predicate)
        return self.child.optimize_selection(filter)

    def optimize_projection(self, fields):
        # Do we have to add fields for filtering, if so, we have to remove them after
        # otherwise we can just swap operators
        keys = self.filters.keys()
        self.child = self.child.optimize_projection(fields | keys)
        self.query.fields = fields
        if not keys <= fields:
            # XXX add projection that removed added_fields
            # or add projection that removes fields
            old_self_callback = self.get_callback()
            projection = Projection(self, fields)
            projection.set_callback(old_self_callback)
            return projection
        return self
