from manifold.core.filter          import Filter
from manifold.operators.operator   import Operator
from manifold.core.packet          import QueryPacket
from manifold.core.record          import Record
from manifold.operators.selection  import Selection
from manifold.operators.projection import Projection
from manifold.util.predicate       import Predicate, eq, included
from manifold.util.type            import returns
from manifold.util.log             import Log

# XXX No more support for list as a child
# XXX Manage callbacks
# XXX Manage query 
# XXX Do we still need inject ?

#------------------------------------------------------------------
# LEFT JOIN node
#------------------------------------------------------------------

class LeftJoin(Operator):
    """
    LEFT JOIN operator node
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, predicate, parent_producer, producers):
        """
        \brief Constructor
        \param left_child  A Node instance corresponding to left  operand of the LEFT JOIN
        \param right_child A Node instance corresponding to right operand of the LEFT JOIN
        \param predicate A Predicate instance invoked to determine whether two record of
            left_child and right_child can be joined.
        \param callback The callback invoked when the LeftJoin instance returns records. 
        """

        # Check parameters
        #assert issubclass(type(right_child), Node), "Invalid right child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(predicate))
        assert predicate.op == eq
        # In fact predicate is always : object.key, ==, VALUE

        # Initialization
        super(LeftJoin, self).__init__(producers, parent_producer, max_producers = 2, has_parent_producer = True)
        self._predicate = predicate

        self._left_map     = {}
        self._left_done    = False
        self._right_packet = None


    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    def __repr__(self):
        return "LEFT JOIN %s %s %s" % self.predicate.get_str_tuple()


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """

        if packet.get_type() == Packet.TYPE_QUERY:
            # We forward the query to the left node
            # TODO : a subquery in fact

            left_packet        = packet.clone()
            self._right_packet = packet.clone() 

            self._producers.send_parent(packet)

        elif packet.get_type() == Packet.TYPE_RECORD:
            record = packet

            if packet.get_source() == self._producers.get_parent_producer(): # XXX
                # formerly left_callback()
                if record.is_last():
                    # We have all left records

                    # NOTE: We used to dynamically change the query plan to
                    # filter on the primary key, which is not efficient. since
                    # the filter will always go deep down to the FROM node.

                    self._left_done = True

                    keys = self._left_map.keys()
                    predicate = Predicate(self.predicate.get_value(), included, self.left_map.keys())

                    query = self.right_packet.get_query().filter_by(predicate)
                    self.right_packet.set_query(query) # XXX

                    self.send(self.right_packet) # XXX
                    return

                if not record.has_fields(self.predicate.get_field_names()):
                    Log.warning("Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                            (self.predicate, record))
                    self.send(record)
                    return

                # Store the result in a hash for joining later
                hash_key = record.get_value(self.predicate.get_key())
                if not hash_key in self._left_map:
                    self._left_map[hash_key] = []
                self._left_map[hash_key].append(record)

            else:
                # formerly right_callback()

                if record.is_last():
                    # Send records in left_results that have not been joined...
                    for left_record_list in self.left_map.values():
                        for left_record in left_record_list:
                            self.send(left_record)

                    # ... and terminates
                    self.send(record)
                    return

                # Skip records missing information necessary to join
                if not set(self.predicate.get_value()) <= set(record.keys()) \
                or record.is_empty(self.predicate.get_value()):
                    Log.warning("Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                            (self.predicate, record))
                    # XXX Shall we send ICMP ?
                    return
                
                # We expect to receive information about keys we asked, and only these,
                # so we are confident the key exists in the map
                # XXX Dangers of duplicates ?
                key = record.get_value(self.predicate.get_value())
                left_records = self._left_map.pop(key)
                for left_record in left_records:
                    left_record.update(record)
                    self.send(left_record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        Node.dump(indent)
        # We have one producer for sure
        self.get_producer().dump()

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
            

    #---------------------------------------------------------------------------
    # Deprecated code 
    #---------------------------------------------------------------------------

#        if isinstance(left_child, list):
#            self.left_done = True
#            for r in left_child:
#                if isinstance(r, dict):
#                    self.left_map[Record.get_value(r, self.predicate.get_key())] = r
#                else:
#                    # r is generally a tuple
#                    self.left_map[r] = Record.from_key_value(self.predicate.get_key(), r)
#        else:
#            old_cb = left_child.get_callback()
#            #Log.tmp("Set left_callback on node ", left_child)
#            left_child.set_callback(self.left_callback)
#            self.set_callback(old_cb)
#
#        #Log.tmp("Set right_callback on node ", right_child)
#        right_child.set_callback(self.right_callback)
#
#        if isinstance(left_child, list):
#            self.query = self.right.get_query().copy()
#            # adding left fields: we know left_child is always a dict, since it
#            # holds more than the key only, since otherwise we would not have
#            # injected but only added a filter.
#            if left_child:
#                self.query.fields |= left_child[0].keys()
#        else:
#            self.query = self.left.get_query().copy()
#            self.query.filters |= self.right.get_query().filters
#            self.query.fields  |= self.right.get_query().fields
#        
#
#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields
#
#
#    @returns(list)
#    def get_children(self):
#        return [self.left, self.right]
#
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
#        
#    #@returns(LeftJoin)
#    def inject(self, records, key, query):
#        """
#        \brief Inject record / record keys into the node
#        \param records A list of dictionaries representing records,
#                       or a list of record keys
#        \returns This node
#        """
#
#        if not records:
#            return
#        record = records[0]
#
#        # Are the records a list of full records, or only record keys
#        is_record = isinstance(record, dict)
#
#        if is_record:
#            records_inj = []
#            for record in records:
#                proj = do_projection(record, self.left.query.fields)
#                records_inj.append(proj)
#            self.left = self.left.inject(records_inj, key, query) # XXX
#            # TODO injection in the right branch: only the subset of fields
#            # of the right branch
#            return self
#
#        # TODO Currently we support injection in the left branch only
#        # Injection makes sense in the right branch if it has the same key
#        # as the left branch.
#        self.left = self.left.inject(records, key, query) # XXX
#        return self
#
#    def start(self):
#        """
#        \brief Propagates a START message through the node
#        """
#        # If the left child is a list of record, we can run the right child
#        # right now. Otherwise, we run the right child once every records
#        # from the left child have been fetched (see left_callback)
#        node = self.right if self.left_done else self.left
#        node.start()

