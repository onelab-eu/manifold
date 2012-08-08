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
        self.started = False

class ProcessingNode(Node):
    """
    A processing node is exactly what forms an AST
    """

    def __init__(self):
        self.children = []



class SelectionNode(Node):
    """
    """

    def __init__(self, filters, callback):
        self._filters = filters
        self._callback = callback

    def callback(self, record):
        try:
            if not record:
                self._callback(record)
                return 
            if not self._filters or self._filters.match(record):
                self._callback(record)
        except Exception, e:
            print "Exception during SelectionNode::callback(%r): %s" % (record, e)
            traceback.print_exc()
        


class ProjectionNode(Node):
    """
    """

    def __init__(self, fields, callback):
        self._fields = fields
        self._callback = callback

    def callback(self, record):
        try:
            if not record:
                self._callback(record)
                return
            ret = {}
            for k, v in record.items():
                if k in self._fields:
                    ret[k] = v
            if ret:
                self._callback(ret)
        except Exception, e:
            print "Exception during ProjectionNode::callback(%r): %s" % (record, e)
            traceback.print_exc()
        


class JoinNode(Node):
    """
    """

    def __init__(self, predicate, callback):
        self._callback = callback
        self._predicate = predicate
        self.right_map = {}
        self.right_done = False
        self.left_done = False
        self.left_table = []

    def left_callback(self, record):
        try:
            if not record:
                self.left_done = True
                if self.right_done:
                    self._callback(None) 
                return
            # New result from the left operator
            if self.right_done:
                if self.left_table:
                    self.process_left_table()
                    self.left_table = []
                self.process_left_record(record)
            else:
                self.left_table.append(record)
        except Exception, e:
            print "Exception during JoinNode::left_callback(%r): %s" % (record, e)
            traceback.print_exc()

    def process_left_record(self, record):
        # We cannot join because the left has no key
        if self._predicate not in record:
            self._callback(record)
            return
        if record[self._predicate] in self.right_map:
            record.update(self.right_map[record[self._predicate]])
            del self.right_map[record[self._predicate]]
        self._callback(record)
        # Handling remaining values from JOIN
        # XXX This is not a left join !!
        #for r in self.right_map.values():
        #    yield r
        #print "left[%s] = %d, right[%s] = %d" % (self._left._node, cptl, self._right._node, cptr)

    def process_left_table(self):
        for record in self.left_table:
            self.process_left_record(record)

    def right_callback(self, record):
        try:
            #print "right callback", record
            # We need to send a NULL record to signal the end of the table
            if not record:
                self.right_done = True
                if self.left_done:
                    self.process_left_table()
                    self._callback(None)
                return
            # New result from the right operator
            #
            # Let's build a map according to predicate = simple field name to begin with
            if self._predicate not in record or not record[self._predicate]:
                # We skip records with missing join information
                return
            self.right_map[record[self._predicate]] = record
        except Exception, e:
            print "Exception during JoinNode::right_callback(%r): %s" % (record, e)
            traceback.print_exc()
