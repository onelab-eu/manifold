from types                   import StringTypes
from manifold.core.filter    import Filter
from manifold.operators      import Node, ChildStatus, ChildCallback, LAST_RECORD
from manifold.util.predicate import Predicate, eq, contains, included
from manifold.util.log       import Log

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
        Log.warning("key argument is deprecated")
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
        parent.set_callback(self.parent_callback)

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

        super(SubQuery, self).__init__()

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

                #parent_query = self.parent.get_query()
                #child_query  = child.get_query()
                #parent_fields = parent_query.fields
                #child_fields = child_query.fields
                #intersection = parent_fields & child_fields

                # The operation to be performed is understood only be looking at the predicate
                predicate = self.relations[i].get_predicate()
                Log.debug("child %r, predicate=%r" % (child, predicate))

                key, op, value = predicate.get_tuple()
                if op == eq:
                    # 1..N
                    # Example: parent has slice_hrn, resource has a reference to slice
                    parent_ids = [record[key] for record in self.parent_output]
                    predicate = Predicate(value, included, parent_ids)

                    # Injecting predicate: TODO use optimize_selection()
                    old_child_callback= child.get_callback()
                    self.children[i] = child.optimize_selection(Filter().filter_by(predicate))
                    self.children[i].set_callback(old_child_callback)

                    print "INJECT"
                    self.dump(indent=2)

                elif op == contains:
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

                #print "AST before run_children"
                #self.dump()
                
    #            # (1) in the parent, we might have a field named after the child
    #            # method containing either records or identifiers of the children
    #            if child_query.object in parent_query.fields:
    #                # WHAT DO WE NEED TO DO
    #                # We have the parent: it has a list of records/record keys which are the ones to fetch
    #                # (whether it is 1..1 or 1..N)
    #                # . if it is only keys: add a where
    #                # . otherwise we need to inject records (and reprogram injection in a complex query plane)
    #                #   (based on a left join)
    #                    
    #            elif intersection: #parent_fields <= child_query.fields:
    #                # Case (2) : the child has a backreference to the parent
    #                # For each parent, we need the set of child that point to it...
    #                # We can inject a where limiting the set of explored children to those found in parent
    #                #
    #                # Let's take into account the fact that the parent key can be composite
    #                # (That's complicated to make the filter for composite keys) -- need OR
    #
    #                if len(intersection) == 1:
    #                    # single field. let's collect parent values
    #                    field = iter(intersection).next()
    #                    parent_ids = [record[field] for record in self.parent_output]
    #                else:
    #                    # multiple filters: we use tuples
    #                    field = tuple(intersection)
    #                    parent_ids = [tuple([record[f] for f in field]) for record in self.parent_output]
    #                    
    #                # We still need to inject part of the records, LEFT JOIN tout ca...
    #                predicate = Predicate(field, '==', parent_ids)
    #                print "INJECTING PREDICATE", predicate
    #                old_child_callback= child.get_callback()
    #                where = Selection(child, Filter().filter_by(predicate))
    #                where.query = child.query.copy().filter_by(predicate)
    #                where.set_callback(child.get_callback())
    #                #self.children[i] = where
    #                self.children[i] = where.optimize()
    #                self.children[i].set_callback(old_child_callback)
    #

            # We make another loop since the children might have been modified in
            # the previous one.
            for i, child in enumerate(self.children):
                self.status.started(i)
            for i, child in enumerate(self.children):
                Log.debug("Starting child %r" % child)
                child.start()
        except Exception, e:
            print "EEE:", e
            import traceback
            traceback.print_exc()

    def all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """

        for o in self.parent_output:
            # Dispatching child results
            for i, child in enumerate(self.children):

                predicate = self.relations[i].get_predicate()
                Log.debug("child %r, predicate=%r" % (child, predicate))

                key, op, value = predicate.get_tuple()
                if op == eq:
                    # 1..N
                    # Example: parent has slice_hrn, resource has a reference to slice
                    #            PARENT       CHILD
                    # Predicate: (slice_hrn,) == slice

                    # Collect in parent all child such as they have a pointer to the parent
                    if isinstance(key, StringTypes):
                        # simple key
                        filter = Filter().filter_by(Predicate(value, eq, o[key]))
                    else:
                        # Composite key, o[value] is a dictionary
                        filter = Filter()
                        for field in value:
                            filter = filter.filter_by(Predicate(field, eq, o[value][field])) # o[value] might be multiple

                    o[predicate.get_key()] = []
                    for child_record in self.child_results[i]:
                        if filter.match(child_record):
                            o[predicate.get_key()].append(child_record)

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

    def child_callback(self, child_id, record):
        """
        \brief Processes records received by a child node
        \param record dictionary representing the received record
        """
        #Log.tmp(record)
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
        # SUBQUERY
        parent_filter = Filter()
        for predicate in filter:
            print "predicate.key", predicate.key
            print "self.parent.get_query().fields", self.parent.get_query().fields
            if predicate.key in self.parent.get_query().fields:
                parent_filter.add(predicate)
            else:
                Log.warning("SubQuery::optimize_selection() is only partially implemented : %r" % predicate)

        if parent_filter:
            self.parent = self.parent.optimize_selection(parent_filter)
            self.parent.set_callback(self.parent_callback)
        return self

    def optimize_projection(self, fields):
        parent_keys = set()
        child_key = []
        child_fields = []
        parent_fields = False

        for i, child in enumerate(self.children):
            predicate = self.relations[i].get_predicate()
            parent_keys   |= predicate.get_field_names()
            parent_fields &= not parent_keys <= fields

            child_key.append(predicate.get_value_names())
            child_fields  = fields & child.get_query().get_select()
            child_fields |= child_key[i]

            self.children[i] = child.optimize_projection(child_fields)

        if parent_fields:
            old_self_callback = self.get_callback()
            projection = Projection(self, fields)
            projection.set_callback(old_self_callback)
            return projection
        return self
            
