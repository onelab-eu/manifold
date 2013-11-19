from manifold.core.packet           import Packet
from manifold.core.record           import Record
from manifold.core.node             import Node
from manifold.operators.operator    import Operator
from manifold.util.type             import returns

DUMPSTR_PROJECTION = "SELECT %s" 

#------------------------------------------------------------------
# Shared utility function
#------------------------------------------------------------------

def do_projection(record, fields):
    """
    Take the necessary fields in dic
    """
    ret = Record()

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
        if not method in record:
            continue
        for x in record[method]:
            arr.append(do_projection(x, subfields))
        ret[method] = arr

    return ret

#------------------------------------------------------------------
# PROJECTION node
#------------------------------------------------------------------

class Projection(Operator):
    """
    PROJECTION operator node (cf SELECT clause in SQL)
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, fields):
        """
        \brief Constructor
        \param child A Node instance which will be the child of
            this Node.
        \param fields A list of Field instances corresponding to
            the fields we're selecting.
        """
        Operator.__init__(self, producers = child, max_producers = 1)

        #for field in fields:
        #    assert isinstance(field, Field), "Invalid field %r (%r)" % (field, type(field))

        if isinstance(fields, (list, tuple, frozenset)):
            fields = set(fields)
        self._fields = fields

#DEPRECATED|        # Callbacks
#DEPRECATED|        old_cb = child.get_callback()
#DEPRECATED|        child.set_callback(self.child_callback)
#DEPRECATED|        self.set_callback(old_cb)
#DEPRECATED|
#DEPRECATED|        self.query = self.child.get_query().copy()
#DEPRECATED|        self.query.fields &= fields

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(set)
    def get_fields(self):
        """
        \returns The list of Field instances selected in this node.
        """
        return self._fields


    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    def __repr__(self):
        return DUMPSTR_PROJECTION % ", ".join(self.get_fields())


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """

        if packet.get_type() == Packet.TYPE_QUERY:
            self.send(packet)

        elif packet.get_type() == Packet.TYPE_RECORD:
            record = packet
            if not record.is_last():
                record = do_projection(record, self._fields)
            self.send(record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        # We have one producer for sure
        self.get_producer().dump(indent + 1)


    def optimize_selection(self, query, filter):
        producer = self.get_producer().optimize_selection(query, filter)
        self.get_producer(producer)
        return self

    def optimize_projection(self, query, fields):
        # We only need the intersection of both
        return self.child.optimize_projection(query, self._fields & fields)
