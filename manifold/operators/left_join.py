from manifold.core.filter          import Filter
from manifold.core.record          import Record, LastRecord
from manifold.operators            import Node
from manifold.operators.selection  import Selection
from manifold.operators.projection import Projection
from manifold.util.predicate       import Predicate, eq, included
from manifold.util.type            import returns
from manifold.util.log             import Log

#------------------------------------------------------------------
# LEFT JOIN node
#------------------------------------------------------------------

class LeftJoin(Node):
    """
    LEFT JOIN operator node
    """

    @staticmethod
    def check_init(left_child, right_child, predicate): #, callback):
        #assert issubclass(type(left_child),  Node), "Invalid left child = %r (%r)"  % (left_child,  type(left_child))
        assert issubclass(type(right_child), Node), "Invalid right child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate),    "Invalid predicate = %r (%r)"   % (predicate,   type(predicate))

    def __init__(self, left_child, right_child, predicate):#, callback):
        """
        \brief Constructor
        \param left_child  A Node instance corresponding to left  operand of the LEFT JOIN
        \param right_child A Node instance corresponding to right operand of the LEFT JOIN
        \param predicate A Predicate instance invoked to determine whether two record of
            left_child and right_child can be joined.
        \param callback The callback invoked when the LeftJoin instance returns records. 
        """
        assert predicate.op == eq

        super(LeftJoin, self).__init__()

        # Check parameters
        LeftJoin.check_init(left_child, right_child, predicate)#, callback)

        # Initialization
        self.left      = left_child
        self.right     = right_child 
        self.predicate = predicate
#        self.set_callback(callback)
        self.left_map  = {}
        if isinstance(left_child, list):
            self.left_done = True
            for r in left_child:
                if isinstance(r, dict):
                    self.left_map[Record.get_value(r, self.predicate.key)] = r
                else:
                    # r is generally a tuple
                    self.left_map[r] = Record.from_key_value(self.predicate.key, r)
        else:
            old_cb = left_child.get_callback()
            self.left_done = False
            #Log.tmp("Set left_callback on node ", left_child)
            left_child.set_callback(self.left_callback)
            self.set_callback(old_cb)

        #Log.tmp("Set right_callback on node ", right_child)
        right_child.set_callback(self.right_callback)

        if isinstance(left_child, list):
            self.query = self.right.get_query().copy()
            # adding left fields: we know left_child is always a dict, since it
            # holds more than the key only, since otherwise we would not have
            # injected but only added a filter.
            if left_child:
                self.query.fields |= left_child[0].keys()
        else:
            self.query = self.left.get_query().copy()
            self.query.filters |= self.right.get_query().filters
            self.query.fields  |= self.right.get_query().fields
        

#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields


    @returns(list)
    def get_children(self):
        return [self.left, self.right]

#    @returns(Query)
#    def get_query(self):
#        """
#        \return The query representing AST reprensenting the AST rooted
#            at this node.
#        """
#        print "LeftJoin::get_query()"
#        q = Query(self.get_children()[0])
#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields
#        return q
        
    #@returns(LeftJoin)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records A list of dictionaries representing records,
                       or a list of record keys
        \returns This node
        """

        if not records:
            return
        record = records[0]

        # Are the records a list of full records, or only record keys
        is_record = isinstance(record, dict)

        if is_record:
            records_inj = []
            for record in records:
                proj = do_projection(record, self.left.query.fields)
                records_inj.append(proj)
            self.left = self.left.inject(records_inj, key, query) # XXX
            # TODO injection in the right branch: only the subset of fields
            # of the right branch
            return self

        # TODO Currently we support injection in the left branch only
        # Injection makes sense in the right branch if it has the same key
        # as the left branch.
        self.left = self.left.inject(records, key, query) # XXX
        return self

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # If the left child is a list of record, we can run the right child
        # right now. Otherwise, we run the right child once every records
        # from the left child have been fetched (see left_callback)
        node = self.right if self.left_done else self.left
        node.start()

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(self, indent)
        if isinstance(self.left, list):
            self.tab(indent),
            print '[DATA]', self.left_map.values()
        else:
            self.left.dump(indent + 1)
        self.right.dump(indent + 1)

    def __repr__(self):
        return "LEFT JOIN %s %s %s" % self.predicate.get_str_tuple()

    def left_callback(self, record):
        """
        \brief Process records received by the left child
        \param record A dictionary representing the received record 
        """
        #Log.tmp("left_callback: record = %r" % record)
        if record.is_last():
            # left_done. Injection is not the right way to do this.
            # We need to insert a filter on the key in the right member
            predicate = Predicate(self.predicate.get_value(), included, self.left_map.keys())
            
            self.right = self.right.optimize_selection(Filter().filter_by(predicate))
            self.right.set_callback(self.right_callback) # already done in __init__ ?

            self.left_done = True
            #Log.tmp("starting right child", self.right)
            self.right.start()
            return

        # Directly send records missing information necessary to join
        # XXXX !!! XXX XXX XXX
        if not Record.has_fields(record, self.predicate.get_field_names()):
            Log.tmp("toto %s" % self.predicate.get_field_names())
            Log.warning("toto Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                    (self.predicate, record))
            self.send(record)

        # Store the result in a hash for joining later
        hash_key = Record.get_value(record, self.predicate.key)
        if not hash_key in self.left_map:
            self.left_map[hash_key] = []
        self.left_map[hash_key].append(record)

    def right_callback(self, record):
        """
        \brief Process records received from the right child
        \param record A dictionary representing the received record 
        """
        if record.is_last():
            # Send records in left_results that have not been joined...
            for left_record_list in self.left_map.values():
                for left_record in left_record_list:
                    self.send(left_record)

            # ... and terminates
            self.send(LastRecord())
            return

        # Skip records missing information necessary to join
#DEPRECATED|        if self.predicate.value not in record or not record[self.predicate.value]:
        #Log.tmp("%s <= %s" %(set(self.predicate.get_value()) , set(record.keys()))) 
        if not set(self.predicate.get_value()) <= set(record.keys()) \
        or Record.is_empty_record(record, self.predicate.get_value()):
            Log.warning("Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                    (self.predicate, record))
            return
        
        # We expect to receive information about keys we asked, and only these,
        # so we are confident the key exists in the map
        # XXX Dangers of duplicates ?
        key = Record.get_value(record, self.predicate.value)
        for left_record in self.left_map[key]:
            left_record.update(record)
            self.send(left_record)

        del self.left_map[key]

    def optimize_selection(self, filter):
        # LEFT JOIN
        # We are pushing selections down as much as possible:
        # - selection on filters on the left: can push down in the left child
        # - selection on filters on the right: cannot push down
        # - selection on filters on the key / common fields ??? TODO
        parent_filter, left_filter = Filter(), Filter()
        for predicate in filter:
            if predicate.get_field_names() < self.left.get_query().get_select():
                left_filter.add(predicate)
            else:
                parent_filter.add(predicate)

        if left_filter:
            self.left = self.left.optimize_selection(left_filter)
            #selection = Selection(self.left, left_filter)
            #selection.query = self.left.copy().filter_by(left_filter)
            self.left.set_callback(self.left_callback)
            #self.left = selection

        if parent_filter:
            old_self_callback = self.get_callback()
            selection = Selection(self, parent_filter)
            # XXX do we need to set query here ?
            #selection.query = self.query.copy().filter_by(parent_filter)
            selection.set_callback(old_self_callback)
            return selection
        return self

    def optimize_projection(self, fields):
        
        # Ensure we have keys in left and right children
        # After LEFTJOIN, we might keep the left key, but never keep the right key

        key_left = self.predicate.get_field_names()
        key_right = self.predicate.get_value_names()

        left_fields    = fields & self.left.get_query().get_select()
        right_fields   = fields & self.right.get_query().get_select()
        left_fields   |= key_left
        right_fields  |= key_right

        self.left  = self.left.optimize_projection(left_fields)
        self.right = self.right.optimize_projection(right_fields)

        self.query.fields = fields

        if left_fields | right_fields > fields:
            old_self_callback = self.get_callback()
            projection = Projection(self, fields)
            projection.query = self.get_query().copy()
            projection.query.fields = fields
            projection.set_callback(old_self_callback)
            return projection
        return self
            
