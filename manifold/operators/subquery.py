import traceback
from types                         import StringTypes
from manifold.core.filter          import Filter
from manifold.core.relation        import Relation
from manifold.operators            import Node, ChildStatus, ChildCallback, LAST_RECORD
from manifold.operators.selection  import Selection
from manifold.operators.projection import Projection
from manifold.util.predicate       import Predicate, eq, contains, included
from manifold.util.log             import Log

DUMPSTR_SUBQUERIES = "<subqueries>"

#------------------------------------------------------------------
# SUBQUERY node
#------------------------------------------------------------------

class SubQuery(Node):
    """
    SUBQUERY operator (cf nested SELECT statements in SQL)
    """

    def __init__(self, parent, children_ast_relation_list, key): # KEY DEPRECATED
        """
        Constructor
        \param parent
        \param children
        \param key the key for elements returned from the node
        """
        super(SubQuery, self).__init__()
        # Parameters
        self.parent, self.key = parent, key # KEY DEPRECATED

        # Remove potentially None children
        # TODO  how do we guarantee an answer to a subquery ? we should branch
        # an empty FromList at query plane construction
        self.children = []
        self.relations = []
        for ast, relation in children_ast_relation_list:
            self.children.append(ast)
            self.relations.append(relation)

        # Member variables
        self.parent_output = []

        # Set up callbacks
        old_cb = parent.get_callback()
        parent.set_callback(self.parent_callback)
        self.set_callback(old_cb)

        self.query = self.parent.get_query().copy()
        for i, child in enumerate(self.children):
            self.query.fields.add(child.get_query().object)

        # Prepare array for storing results from children: parent result can
        # only be propagated once all children have replied
        self.child_results = []
        self.status = ChildStatus(self.all_done)
        for i, child in enumerate(self.children):
            child.set_callback(ChildCallback(self, i))
            self.child_results.append([])


#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        # Query is unchanged XXX ???
#        return Query(self.parent.get_query())

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print '<main>'
        self.parent.dump(indent+1)
        if not self.children: return
        self.tab(indent)
        print '<subqueries>'
        for child in self.children:
            child.dump(indent + 1)

    def __repr__(self):
        return DUMPSTR_SUBQUERIES

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # Start the parent first
        self.parent.start()

        

    def parent_callback(self, record):
        """
        \brief Processes records received by the parent node
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            # When we have received all parent records, we can run children
            if self.parent_output:
                self.run_children()
            return
        # Store the record for later...
        self.parent_output.append(record)

    def run_children(self):
        """
        \brief Modify children queries to take the keys returned by the parent into account
        """
        try:
            # Loop through children and inject the appropriate parent results
            for i, child in enumerate(self.children):
                # We have two cases:
                # (1) either the parent query has subquery fields (a list of child
                #     ids + eventually some additional information)
                # (2) either the child has a backreference to the parent
                #     ... eventually a partial reference in case of a 1..N relationship
                #
                # In all cases, we will collect all identifiers to proceed to a
                # single child query for efficiency purposes, unless it's not
                # possible (?).
                #
                # We have several parent records stored in self.parent_output
                #
                # /!\ Can we have a mix of (1) and (2) ? For now, let's suppose NO.
                #  *  We could expect key information to be stored in the DBGraph

                # The operation to be performed is understood only be looking at the predicate
                relation = self.relations[i]
                predicate = relation.get_predicate()

                key, op, value = predicate.get_tuple()
                if op == eq:
                    # 1..N
                    # Example: parent has slice_hrn, resource has a reference to slice
                    if relation.get_type() == Relation.types.LINK_1N_BACKWARDS:
                        parent_ids = [record[key] for record in self.parent_output]
                        filter_pred = Predicate(value, included, parent_ids)
                    else:
                        print "self.parent_output=", self.parent_output
                        print "KEY?", key
                        if isinstance(key, tuple):
                            parent_ids = [x for record in self.parent_output if key in record for x in record[key]]
                        else:
                            parent_ids = [elem for record in self.parent_output if key in record for elem in record[key]]
                        Log.tmp("="*80)
                        Log.tmp(parent_ids)
                            
                        if parent_ids and isinstance(parent_ids[0], dict):
                            parent_ids = map(lambda x: x[value], parent_ids)
                        Log.tmp("="*80)
                        Log.tmp(parent_ids)
                        filter_pred = Predicate(value, included, parent_ids)

                    # Injecting predicate
                    old_child_callback= child.get_callback()
                    self.children[i] = child.optimize_selection(Filter().filter_by(filter_pred))
                    self.children[i].set_callback(old_child_callback)

                elif op == contains:
                    Log.tmp("OP == CONTAINS")
                    # 1..N
                    # Example: parent 'slice' has a list of 'user' keys == user_hrn
                    for slice in self.parent_output:
                        if not child.get_query().object in slice: continue
                        users = slice[key]
                        # users est soit une liste d'id, soit une liste de records
                        user_data = []
                        for user in users:
                            if isinstance(user, dict):
                                user_data.append(user)
                            else:
                                # have have a key
                                # XXX Take multiple keys into account
                                user_data.append({value: user}) 
                        # Let's inject user_data in the right child
                        child.inject(user_data, value, None) 
                    
                else:
                    raise Exception, "No link between parent and child queries"

            # We make another loop since the children might have been modified in
            # the previous one.
            for i, child in enumerate(self.children):
                self.status.started(i)
            for i, child in enumerate(self.children):
                child.start()
        except Exception, e:
            print "EEE!", e
            traceback.print_exc()

    def all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """
        try:
            for o in self.parent_output:
                # Dispatching child results
                for i, child in enumerate(self.children):

                    relation = self.relations[i]
                    predicate = relation.get_predicate()

                    key, op, value = predicate.get_tuple()
                    
                    if op == eq:
                        # 1..N
                        # Example: parent has slice_hrn, resource has a reference to slice
                        #            PARENT       CHILD
                        # Predicate: (slice_hrn,) == slice

                        # Collect in parent all child such as they have a pointer to the parent
                        if isinstance(key, StringTypes):
                            # simple key
                            ids = o[key] if key in o else []
                            if ids and isinstance(ids[0], dict):
                                ids = map(lambda x: x[value], ids)
                            filter = Filter().filter_by(Predicate(value, included, ids))
                        else:
                            # Composite key, o[value] is a dictionary
                            filter = Filter()
                            for field in value:
                                filter = filter.filter_by(Predicate(field, included, o[value][field])) # o[value] might be multiple

                        o[relation.get_relation_name()] = []
                        for child_record in self.child_results[i]:
                            if filter.match(child_record):
                                o[relation.get_relation_name()].append(child_record)

                    elif op == contains:
                        # 1..N
                        # Example: parent 'slice' has a list of 'user' keys == user_hrn
                        #            PARENT        CHILD
                        # Predicate: user contains (user_hrn, )

                        # first, replace records by dictionaries. This only works for non-composite keys
                        if o[child.query.object]:
                            record = o[child.query.object][0]
                            if not isinstance(record, dict):
                                o[child.query.object] = [{value: record} for record in o[child.query.object]]

                        if isinstance(value, StringTypes):
                            for record in o[child.query.object]:
                                # Find the corresponding record in child_results and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter().filter_by(Predicate(value, eq, record[value]))
                                    for r in self.child_results[i]:
                                        if filter.match(r):
                                            record.update(r)
                        else:
                            for record in o[child.query.object]:
                                # Find the corresponding record in child_results and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter()
                                    for field in value:
                                        filter = filter.filter_by(Predicate(field, eq, record[field]))
                                    for r in self.child_results[i]:
                                        if filter.match(r):
                                            record.update(r)
                        
                    else:
                        raise Exception, "No link between parent and child queries"

                self.send(o)
            self.send(LAST_RECORD)
        except Exception, e:
            print "EEE", e
            traceback.print_exc()

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by a child node
        \param record dictionary representing the received record
        """
        if record == LAST_RECORD:
            self.status.completed(child_id)
            return
        # Store the results for later...
        self.child_results[child_id].append(record)

    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records, or list of
        """
        raise Exception, "Not implemented"

    def optimize_selection(self, filter):
        parent_filter, top_filter = Filter(), Filter()
        for predicate in filter:
            if predicate.get_field_names() <= self.parent.get_query().get_select():
                parent_filter.add(predicate)
            else:
                Log.warning("SubQuery::optimize_selection() is only partially implemented : %r" % predicate)
                top_filter.add(predicate)

        if parent_filter:
            self.parent = self.parent.optimize_selection(parent_filter)
            self.parent.set_callback(self.parent_callback)

        if top_filter:
            return Selection(self, top_filter)

        return self

    def optimize_projection(self, fields):
        parent_keys = set()
        parent_fields = False

        for i, child in enumerate(self.children[:]):
            relation = self.relations[i]
            name     = relation.get_relation_name()

            my_fields = set()
            for field in fields:
                if not '.' in field: continue
                m, f = field.split('.', 1)
                if m == name:
                    my_fields.add(f)

            predicate = relation.get_predicate()
            parent_keys   |= predicate.get_field_names()
            if not parent_keys <= my_fields:
                parent_fields = True

            child_fields  = my_fields & child.get_query().get_select()
            child_fields |= predicate.get_value_names()

            self.children[i] = child.optimize_projection(child_fields)

        if parent_fields:
            return Projection(self, fields)
        return self
            
