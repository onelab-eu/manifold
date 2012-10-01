import traceback

class Node:
    """
    Implements a Node for a data streaming environment.
    """
    pass


class SourceNode(Node):
    """
    A source node is exactly a gateway that connects to various data sources
    """
    # XXX should be a gateway factory ???

    def __init__(self, callback, platform, query, config):
        self._callback = callback
        self.platform = platform
        self.query = query
        self.config = config
        self.do_start = True
        self.started = False

class ProcessingNode(Node):
    """
    A processing node is exactly what forms an AST
    """

    def __init__(self):
        self.children = []



#class SelectionNode(Node):
#    """
#    """
#
#    def __init__(self, filters, callback):
#        self._filters = filters
#        self._callback = callback
#
#    def callback(self, record):
#
#        try:
#            if not record:
#                self._callback(record)
#                return 
#            if self._filters:
#                record = self._filters.filter(record)
#            self._callback(record)
#        except Exception, e:
#            print "Exception during SelectionNode::callback: %s" % e
#            traceback.print_exc()
#        
#
#
#class ProjectionNode(Node):
#    """
#    """
#
#    def __init__(self, fields, callback):
#        self._fields = fields
#        self._callback = callback
#
#    def callback(self, record):
#
#        def local_projection(record, fields):
#            """
#            Take the necessary fields in dic
#            """
#            ret = {}
#
#            # 1/ split subqueries
#            local = []
#            subqueries = {}
#            for f in fields:
#                if '.' in f:
#                    method, subfield = f.split('.', 1)
#                    if not method in subqueries:
#                        subqueries[method] = []
#                    subqueries[method].append(subfield)
#                else:
#                    local.append(f)
#            
#            # 2/ process local fields
#            for l in local:
#                ret[l] = record[l] if l in record else None
#
#            # 3/ recursively process subqueries
#            for method, subfields in subqueries.items():
#                # record[method] is an array whose all elements must be
#                # filtered according to subfields
#                arr = []
#                for x in record[method]:
#                    arr.append(local_projection(x, subfields))
#                ret[method] = arr
#
#            return ret
#
#        try:
#            if not record:
#                self._callback(record)
#                return
#            ret = local_projection(record, self._fields)
#            self._callback(ret)
#
#        except Exception, e:
#            print "Exception during ProjectionNode::callback(%r): %s" % (record, e)
#            traceback.print_exc()
#        
#
#
#class JoinNode(Node):
#    """
#    """
#
#    def __init__(self, predicate, callback):
#        self._callback = callback
#        self._predicate = predicate
#        self.right_map = {}
#        self.right_done = False
#        self.left_done = False
#        self.left_table = []
#
#    def left_callback(self, record):
#        try:
#            if not record:
#                self.left_done = True
#                if self.right_done:
#                    self._callback(None) 
#                return
#            # New result from the left operator
#            if self.right_done:
#                if self.left_table:
#                    self.process_left_table()
#                    self.left_table = []
#                self.process_left_record(record)
#            else:
#                self.left_table.append(record)
#        except Exception, e:
#            print "Exception during JoinNode::left_callback(%r): %s" % (record, e)
#            traceback.print_exc()
#
#    def process_left_record(self, record):
#        # We cannot join because the left has no key
#        if self._predicate not in record:
#            self._callback(record)
#            return
#        if record[self._predicate] in self.right_map:
#            record.update(self.right_map[record[self._predicate]])
#            del self.right_map[record[self._predicate]]
#        self._callback(record)
#        # Handling remaining values from JOIN
#        # XXX This is not a left join !!
#        #for r in self.right_map.values():
#        #    yield r
#        #print "left[%s] = %d, right[%s] = %d" % (self._left._node, cptl, self._right._node, cptr)
#
#    def process_left_table(self):
#        for record in self.left_table:
#            self.process_left_record(record)
#
#    def right_callback(self, record):
#        try:
#            #print "right callback", record
#            # We need to send a NULL record to signal the end of the table
#            if not record:
#                self.right_done = True
#                if self.left_done:
#                    self.process_left_table()
#                    self._callback(None)
#                return
#            # New result from the right operator
#            #
#            # Let's build a map according to predicate = simple field name to begin with
#            if self._predicate not in record or not record[self._predicate]:
#                # We skip records with missing join information
#                return
#            self.right_map[record[self._predicate]] = record
#        except Exception, e:
#            print "Exception during JoinNode::right_callback(%r): %s" % (record, e)
#            traceback.print_exc()
#
#class SubQueryNode(Node):
#    """
#    """
#    # Can be improved for better asynchronicity
#
#    # In this case, we cannot start all sources, since the children are waiting
#    # keys from the parent
#    
#    def __init__(self, children, router, callback):
#        self._router = router
#        # The callback to be called at the end
#        self._callback = callback
#        # Children in the ast on which to call install
#        self._children = children
#
#        # Build a callback array for all children
#        self.array_callback = []
#        for i in range(0, len(children)):
#            f = lambda record: self.child_callback(i, record)
#            self.array_callback.append(f)
#
#        # List storing parent records
#        self.parent_output = []
#        # Lists storing child records
#
#    def parent_callback(self, record):
#        if record:
#            self.parent_output.append(record)
#            return
#        # When we have received all parent records, we can run children
#        self.run_children()
#
#    def run_children(self):
#        """
#        Modify children queries to take the keys returned by the parent into account
#        """
#        # Loop through children
#        for q in self._children:
#            # Collect keys from parent results
#            parent_keys = []
#            for o in self.parent_output:
#                if q.fact_table in o:
#                    # o[method] can be :
#                    # - an id (1..)
#                    # - an object (1..1)
#                    # - a list of id (1..N)
#                    # - a list of objects (1..N)
#                    if isinstance(o[q.fact_table], list):
#                        # We inspected each returned object or key
#                        for x in o[q.fact_table]:
#                            if isinstance(x, dict):
#                                # - get key from metadata and add it
#                                # - What to do with remaining fields, we
#                                #   might not be able to get them
#                                #   somewhere else
#                                raise Exception, "returning subobjects not implemented (yet)."
#                            else:
#                                parent_keys.append(x)
#                    else:
#                        # 1..1
#                        raise Exception, "1..1 relationships are not implemented (yet)."
#                    parent_keys.extend(o['key']) # 1..N
#
#            # Add filter on method key
#            keys = self._router.metadata_get_keys(q.fact_table)
#            if not keys:
#                raise Exception, "Cannot complete query: submethod %s has no key" % method
#            key = list(keys).pop()
#            if q.filters.has_key(key):
#                raise Exception, "Filters on keys are not allowed (yet) for subqueries"
#            # XXX careful frozen sets !!
#            q.filters.add(Predicate(key, '=', parent_keys))
#
#        # Run child nodes
#        # XXX TODO
#
#    def child_callback(self, child_id, record):
#
#        # When we have all results !
#        self._callback(merged_parent_record)
#
