from manifold.operators            import Node, ChildStatus, ChildCallback
from manifold.operators.projection import Projection
from manifold.core.record          import Record, LastRecord
from manifold.util.log             import Log

DUMPSTR_UNION      = "UNION"

#------------------------------------------------------------------
# UNION node
#------------------------------------------------------------------
            
class Union(Node):
    """
    UNION operator node
    """

    def __init__(self, children, key):
        """
        \brief Constructor
        \param children A list of Node instances, the children of
            this Union Node.
        \param key A Key instance, corresponding to the key for
            elements returned from the node
        """
        super(Union, self).__init__()
        # Parameters
        self.children = list()
        self.key = key
        # Member variables
        #self.child_status = 0
        #self.child_results = {}
        # Stores the list of keys already received to implement DISTINCT
        self.key_map = dict()
        self.status = ChildStatus(self.all_done)

        self.add_children(children)

        # We suppose all children have the same format...
        # NOTE: copy is important otherwise we use the same
        self.query = self.children[0].get_query().copy()

    def add_children(self, children):
        num_children = len(self.children)

        self.children.extend(children)
        # callbacks
        # Set up callbacks
        for i, child in enumerate(children):
            child.set_callback(ChildCallback(self, num_children + i))
        

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        # We suppose all child queries have the same format, and that we have at
#        # least one child
#        print "Union::get_query()"
#        return Query(self.children[0].get_query())
        
    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        s = []
        s.append(Node.dump(self, indent))
        for child in self.children:
            s.append(child.dump(indent + 1))
        return '\n'.join(s)

    def __repr__(self):
        return DUMPSTR_UNION

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start all children
        for i, child in enumerate(self.children):
            self.status.started(i)
        for i, child in enumerate(self.children):
            child.start()

    def inject_insert(self, params):
        for i, child in enumerate(self.children):
            self.children[i] = child.inject_insert(params)

    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        record keys
        """
        for i, child in enumerate(self.children):
            self.children[i] = child.inject(records, key, query)
        return self

    def all_done(self):
        for record in self.key_map.values():
            self.send(record)
        self.send(LastRecord())

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by the child node
        \param child_id identifier of the child that received the record
        \param record dictionary representing the received record
        """
        if record.is_last():
            # XXX SEND ALL
            self.status.completed(child_id)
            return
        
        key = self.key.get_field_names()

        # DISTINCT not implemented, just forward the record
        if not key:
            Log.critical("No key associated to UNION operator")
            self.send(record)
            return

        # Send records that have no key
        if not Record.has_fields(record, key):
            Log.info("UNION::child_callback sent record without key '%(key)s': %(record)r", **locals())
            self.send(record)
            return

        key_value = Record.get_value(record, key)
        
        if key_value in self.key_map:
            Log.debug("UNION::child_callback merged duplicate records: %r" % record)
            prev_record = self.key_map[key_value]
            for k, v in record.items():
                if not k in prev_record:
                    prev_record[k] = v
                    continue
                if isinstance(v, list):
                    if not prev_record[k]:
                        prev_record[k] = list() # with failures it can occur that this is None
                    prev_record[k].extend(v) # DUPLICATES ?
                #else:
                #    if not v == previous[k]:
                #        print "W: ignored conflictual field"
                #    # else: nothing to do
        else:
            self.key_map[key_value] = record

    def optimize_selection(self, filter):
        # UNION: apply selection to all children
        for i, child in enumerate(self.children):
            old_child_callback= child.get_callback()
            self.children[i] = child.optimize_selection(filter)
            self.children[i].set_callback(old_child_callback)
        return self

    def optimize_projection(self, fields):
        # UNION: apply projection to all children
        # XXX in case of UNION with duplicate elimination, we need the key
        # until then, apply projection to all children
        #self.query.fields = fields
        do_parent_projection = False

        #if self.distinct:
        key = self.key.get_field_names()
        if key not in fields: # we are not keeping the key
            do_parent_projection = True
            child_fields  = set()
            child_fields |= fields
            child_fields |= key

        for i, child in enumerate(self.children):
            old_child_callback= child.get_callback()
            self.children[i] = child.optimize_projection(child_fields)
            self.children[i].set_callback(old_child_callback)
        if do_parent_projection:
            old_self_callback = self.get_callback()
            projection = Projection(self, fields)
            projection.set_callback(old_self_callback)
            return projection
        return self
