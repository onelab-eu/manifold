from manifold.operators import Node
from manifold.util.type import returns

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

        self.query = None

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
        if not record.is_last():
            #record = { self.map_fields.get(k, k): v for k, v in record.items() }
            try:
                for k, v in self.map_fields.items():
                    if k in record:
                        if '.' in v: # users.hrn
                            method, key = v.split('.')
                            if not method in record:
                                record[method] = []
                            for x in record[k]:
                                record[method].append({key: x})        
                        else:
                            record[v] = record.pop(k) #record[k]
                        #del record[k]
            except Exception, e:
                print "EEE RENAME", e
                import traceback
                traceback.print_exc()
        self.send(record)

    def optimize_selection(self, filter):
        Log.critical('Not implemented')
        return self

    def optimize_rename(self, fields):
        Log.critical('Not implemented')
        return self
