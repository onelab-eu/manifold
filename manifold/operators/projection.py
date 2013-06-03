from manifold.operators import Node, LAST_RECORD
from manifold.util.type import returns

DUMPSTR_PROJECTION = "SELECT %s" 

#------------------------------------------------------------------
# Shared utility function
#------------------------------------------------------------------

def do_projection(record, fields):
    """
    Take the necessary fields in dic
    """
    ret = {}

    # 1/ split subqueries
    local = []
    subqueries = {}
    for f in fields:
        if '.' in f:
            method, subfield = f.split('.', 1)
            if not method in subqueries:
                subqueries[method] = []
            subqueries[method].append(subfield)
        else:
            local.append(f)
    
    # 2/ process local fields
    for l in local:
        ret[l] = record[l] if l in record else None

    # 3/ recursively process subqueries
    for method, subfields in subqueries.items():
        # record[method] is an array whose all elements must be
        # filtered according to subfields
        arr = []
        for x in record[method]:
            arr.append(local_projection(x, subfields))
        ret[method] = arr

    return ret

#------------------------------------------------------------------
# PROJECTION node
#------------------------------------------------------------------

class Projection(Node):
    """
    PROJECTION operator node (cf SELECT clause in SQL)
    """

    def __init__(self, child, fields):
        """
        \brief Constructor
        \param child A Node instance which will be the child of
            this Node.
        \param fields A list of Field instances corresponding to
            the fields we're selecting.
        """
        #for field in fields:
        #    assert isinstance(field, Field), "Invalid field %r (%r)" % (field, type(field))
        if isinstance(fields, (list, tuple, frozenset)):
            fields = set(fields)
        self.child, self.fields = child, fields
        self.child.set_callback(self.get_callback())

        self.query = self.child.get_query().copy()
        self.query.fields &= fields

        super(Projection, self).__init__()

    @returns(set)
    def get_fields(self):
        """
        \returns The list of Field instances selected in this node.
        """
        return self.fields

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return The Query representing the data produced by the nodes.
#        """
#        print "Projection()::get_query()"
#        q = Query(self.child.get_query())
#
#        # Projection restricts the set of available fields (intersection)
#        q.fields &= fields
#        return q

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        self.child.dump(indent+1)

    def __repr__(self):
        return DUMPSTR_PROJECTION % ", ".join(self.get_fields())

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()

    #@returns(Projection)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records A list of dictionaries representing records,
                       or a list of record keys
        \return This node
        """
        self.child = self.child.inject(records, key, query) # XXX
        return self

    def callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record != LAST_RECORD:
            record = do_projection(record, self.fields)
        self.send(record)

    def optimize_selection(self, filter):
        print "W: Projection::optimize_selection() not implemented"
        return self

    def optimize_projection(self, fields):
        # We only need the intersection of both
        return self.optimize_projection(self.fields & fields)
