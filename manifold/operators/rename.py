from types                import StringTypes
from manifold.core.record import Record
from manifold.operators   import Node
from manifold.util.type   import returns
from manifold.util.log    import Log

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

class Rename(Node):
    """
    RENAME operator node (cf SELECT clause in SQL)
    """

    def __init__(self, child, map_fields):
        """
        \brief Constructor
        """
        super(Rename, self).__init__()
        self.child, self.map_fields = child, map_fields

        # Callbacks
        old_cb = child.get_callback()
        child.set_callback(self.child_callback)
        self.set_callback(old_cb)

        self.query = self.child.get_query().copy()
        # XXX Need to rename some fields !!

    @returns(dict)
    def get_map_fields(self):
        """
        \returns The list of Field instances selected in this node.
        """
        return self.map_fields

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        self.child.dump(indent+1)

    def __repr__(self):
        return DUMPSTR_RENAME % self.get_map_fields()

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()

    def child_callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record.is_last():
            self.send(record)
            return

        for k, v in self.map_fields.items():
            if k in record:
                tmp = record.pop(k)
                if '.' in v: # users.hrn
                    method, key = v.split('.')
                    if not method in record:
                        record[method] = []
                    # ROUTERV2
                    if isinstance(tmp, StringTypes):
                        record[method] = {key: tmp}
                    else:
                        for x in tmp:
                            record[method].append({key: x})        
                else:
                    record[v] = tmp
        self.send(record)

    def optimize_selection(self, filter):
        self.child = self.child.optimize_selection(filter)
        return self

    def optimize_projection(self, fields):
        rmap = { v : k for k, v in self.map_fields.items() }
        new_fields = set()
        for field in fields:
            new_fields.add(rmap.get(field, field))
        self.child = self.child.optimize_projection(new_fields)
        return self
