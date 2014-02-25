#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A SubQuery Node applies a posteriori a Query on a
# set of Records already fetched.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import traceback
from types                          import StringTypes

from manifold.core.filter           import Filter
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.relation         import Relation
from manifold.core.record           import Record
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.util.log              import Log
from manifold.util.predicate        import Predicate, eq, contains, included
from manifold.util.type             import accepts, returns

DUMPSTR_SUBQUERIES = "<subqueries>"

#------------------------------------------------------------------
# SUBQUERY node
#------------------------------------------------------------------

class SubQuery(Operator):
    """
    SUBQUERY operator (cf nested SELECT statements in SQL)
        self.parent represents the main query involved in the SUBQUERY operation.
        self.children represents each subqueries involved in the SUBQUERY operation.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, parent_producer, producer_relation_list):
        """
        Constructor
        Args:
            parent: The main query (AST instance ?)
            children_ast_relation_list: A list of (AST , Relation) tuples
        """
        # Check parameters
        Log.error("SubQuery::__init__: not yet migrated")
        producers = []
        self.relations = []
        for producer, relation in producer_relation_list:
            producers.append(producer)
            self.relations.append(relation)

        # Initialization
        super(SubQuery, self).__init__(producers, parent_producer, has_parent_producer = True)

        # Member variables
        self.parent_output = []

        # Prepare array for storing results from children: parent result can
        # only be propagated once all children have replied
        # XXX
        self.child_results = []
        self.status = ChildStatus(self.all_done)
        for producer in producers:
            self.child_results.append([])

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this SubQuery Operator.
        """
        return DUMPSTR_SUBQUERIES

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive_impl(self, packet):
        """
        Handle an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            parent_packet         = packet.clone()
            self._children_packet = packet.clone() 

            self._producers.send_parent(parent_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            if packet.get_source() == self._producers.get_parent_producer(): # XXX
                # formerly parent_callback
                if record.is_last():
                    # When we have received all parent records, we can run children
                    if self.parent_output:
                        self._run_children()
                    return
                # Store the record for later...
                self.parent_output.append(record)

            else:
                # formerly child callback
                if record.is_last():
                    self.status.completed(child_id)
                    return
                # Store the results for later...
                self.child_results[child_id].append(record)


        else: # TYPE_ERROR
            self.send(packet)

#DEPRECATED|    def dump(self, indent = 0):
#DEPRECATED|        """
#DEPRECATED|        Dump the current node
#DEPRECATED|        indent current indentation
#DEPRECATED|        """
#DEPRECATED|        self.tab(indent)
#DEPRECATED|        print '<main>'
#DEPRECATED|        self.parent.dump(indent+1)
#DEPRECATED|        if not self.children: return
#DEPRECATED|        self.tab(indent)
#DEPRECATED|        print '<subqueries>'
#DEPRECATED|        for child in self.children:
#DEPRECATED|            child.dump(indent + 1)

    @staticmethod
    def get_element_key(element, key):
        if isinstance(element, dict):
            # record
            return Record.get_value(element, key)
        else:
            # id or tuple(id1, id2, ...)
            return element

    def _run_children(self):
        """
        Run children queries (subqueries) assuming the parent query (main query)
        has successfully ended.
        """
        if not self.parent_output:
            # No parent record, this is useless to run children queries.
            self.send(Record(last = True))
            return

        if not self.children:
            # The top operator has build a SubQuery node without child queries,
            # so this SubQuery operator is useless and should be replaced by
            # its main query.
            Log.warning("SubQuery::run_children: no child node. The query plan could be improved")
            self.send(Record(last = True))
            return

        # Inspect the first parent record to deduce which fields have already
        # been fetched 
        first_record = self.parent_output[0]
        parent_fields = set(first_record.keys())
        
        # Optimize child queries according to the fields already retrieved thanks
        # to the parent query.
        useless_children = set()
        for i, child in enumerate(self.children[:]):
            # Test whether the current child provides relevant fields (e.g.
            # fields not already fetched in the parent record). If so, reduce
            # the set of queried field in order to only retrieve relevant fields.
            child_fields = child.get_query().get_select()
            relation = self.relations[i]
            relation_name = relation.get_relation_name()
            already_fetched_fields = set()
            if relation_name in parent_fields:
                if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
                    if relation_name in first_record and first_record[relation_name] and len(first_record[relation_name]) > 0:
                        already_fetched_fields = set(first_record[relation_name][0].keys())
                    else:
                        already_fetched_fields = set()
                else:
                    if relation_name in first_record and first_record[relation_name] and len(first_record[relation_name]) > 0:
                        already_fetched_fields = set(first_record[relation_name].keys())
                    else:
                        already_fetched_fields = set()

            # we need to keep key used for subquery
            key_field = relation.get_predicate().get_value()
            relevant_fields = child_fields - already_fetched_fields | frozenset([key_field])

            if not relevant_fields:
                useless_children.add(i)
                continue
            elif child_fields != relevant_fields:
                self.children[i] = child.optimize_projection(relevant_fields)

        # If every children are useless, this means that we already have full records
        # thanks to the parent query, so we simply forward those records.
        if len(self.children) == len(useless_children):
            map(self.send, self.parent_output)
            self.send(Record(last = True))
            return

        # Loop through children and inject the appropriate parent results
        for i, child in enumerate(self.children):
            if i in useless_children: continue

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
                    if len(parent_ids) == 1:
                        parent_id, = parent_ids
                        filter_pred = Predicate(value, eq, parent_id)
                    else:
                        filter_pred = Predicate(value, included, parent_ids)
                else:
                    parent_ids = []
                    for parent_record in self.parent_output:
                        record = Record.get_value(parent_record, key)
                        if not record:
                            record = []
                        if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
                            # we have a list of elements 
                            # element = id or dict    : clé simple
                            #         = tuple or dict : clé multiple
                            parent_ids.extend([self.get_element_key(r, value) for r in record])
                        else:
                            parent_ids.append(self.get_element_key(record, value))
                        
                    #if isinstance(key, tuple):
                    #    parent_ids = [x for record in self.parent_output if key in record for x in record[key]]
                    #else:
                    #    ##### record[key] = text, dict, or list of (text, dict) 
                    #    parent_ids = [record[key] for record in self.parent_output if key in record]
                    #    
                    #if parent_ids and isinstance(parent_ids[0], dict):
                    #    parent_ids = map(lambda x: x[value], parent_ids)

                    if len(parent_ids) == 1:
                        parent_id, = parent_ids
                        filter_pred = Predicate(value, eq, parent_id)
                    else:
                        filter_pred = Predicate(value, included, parent_ids)

                # Injecting predicate
                old_child_callback= child.get_callback()
                self.children[i] = child.optimize_selection(Filter().filter_by(filter_pred))
                self.children[i].set_callback(old_child_callback)

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

        #print "*** before run children ***"
        #self.dump()

        # We make another loop since the children might have been modified in
        # the previous one.
        for i, child in enumerate(self.children):
            if i in useless_children: continue
            self.status.started(i)
        for i, child in enumerate(self.children):
            if i in useless_children: continue
            child.start()

    def all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """
        try:
            for parent_record in self.parent_output:
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
                        record = Record.get_value(parent_record, key)
                        if not record:
                            record = []
                        if not isinstance(record, (list, tuple, set, frozenset)):
                            record = [record]
                        if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
                            # we have a list of elements 
                            # element = id or dict    : clé simple
                            #         = tuple or dict : clé multiple
                            ids = [SubQuery.get_element_key(r, value) for r in record]
                        else:
                            ids = [SubQuery.get_element_key(record, value)]
                        if len(ids) == 1:
                            id, = ids
                            filter = Filter().filter_by(Predicate(value, eq, id))
                        else:
                            filter = Filter().filter_by(Predicate(value, included, ids))

                        #if isinstance(key, StringTypes):
                        #    # simple key
                        #    ids = [o[key]] if key in o else []
                        #    #print "IDS=", ids
                        #    #if ids and isinstance(ids[0], dict):
                        #    #    ids = map(lambda x: x[value], ids)
                        #    # XXX we might have equality instead of IN in case of a single ID
                        #    print "VALUE", value, "INCLUDED ids=", ids
                        #    filter = Filter().filter_by(Predicate(value, included, ids))
                        #else:
                        #    # Composite key, o[value] is a dictionary
                        #    for field in value:
                        #        filter = filter.filter_by(Predicate(field, included, o[value][field])) # o[value] might be multiple

                        parent_record[relation.get_relation_name()] = []
                        for child_record in self.child_results[i]:
                            if filter.match(child_record):
                                # 
                                # parent_record[relation.get_relation_name()]
                                # contains a list of children objects, with some
                                # properties
                                # their key is relation.get_value_names()
                                parent_record[relation.get_relation_name()].append(child_record)
                                

                    elif op == contains:
                        # 1..N
                        # Example: parent 'slice' has a list of 'user' keys == user_hrn
                        #            PARENT        CHILD
                        # Predicate: user contains (user_hrn, )

                        # first, replace records by dictionaries. This only works for non-composite keys
                        if parent_record[child.query.object]:
                            record = parent_record[child.query.object][0]
                            if not isinstance(record, dict):
                                parent_record[child.query.object] = [{value: record} for record in parent_record[child.query.object]]

                        if isinstance(value, StringTypes):
                            for record in parent_record[child.query.object]:
                                # Find the corresponding record in child_results and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter().filter_by(Predicate(value, eq, record[value]))
                                    for r in self.child_results[i]:
                                        if filter.match(r):
                                            record.update(r)
                        else:
                            for record in parent_record[child.query.object]:
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

                self.send(parent_record)
            self.send(Record(last = True))
        except Exception, e:
            print "EEE", e
            traceback.print_exc()

    def optimize_selection(self, filter):
        return self
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
        """
        Propagates projection (SELECT) through a SubQuery node.
        A field is relevant if:
            - it is explicitely queried (i.e. it is a field involved in the projection)
            - it is needed to perform the SubQuery (i.e. it is involved in a Predicate)
        Args:
            fields: A frozenset of String containing the fields involved in the projection.
        Returns:
            The optimized AST once this projection has been propagated.
        """
        return self
        # 1) Determine for the parent and each child which fields are explicitely
        #    queried by the user.
        # 2) Determine for the parent and each child which additionnal fields are
        #    required to join the parent records and the child records.
        # 3) Optimize the parent AST and the child ASTs conseuqently.
        # 4) Filter additionnal fields (see (2)) which have not been queried by
        #    adding a Projection node above the resulting optimized AST.

        # 0) Initialization
        parent_fields = set() # fields returned by the parent query
        child_fields = dict() # fields returned by each child query

        # 1) If the SELECT clause refers to "a.b", this is a Query related to the
        # child subquery related to "a". If the SELECT clause refers to "b" this
        # is always related to the parent query.
        parent_fields = set([field for field in fields if not "." in field]) \
            | set([field.split('.')[0] for field in fields if "." in field])
        for i, child in enumerate(self.children[:]):
            child_name = self.relations[i].get_relation_name()
            child_fields[child_name] = set([field.split('.', 1)[1] for field in fields if field.startswith("%s." % child_name)])

        # 2) Add to child_fields and parent_fields the field names needed to
        # connect the parent to its children. If such fields are added, we will
        # filter them in step (4). Once we have deduced for each child its
        # queried fields (see (1)) and the fields needed to connect it to the
        # parent query (2), we can start the to optimize the projection (3).
        require_top_projection = False
        for i, child in enumerate(self.children[:]):
            relation = self.relations[i]
            predicate = relation.get_predicate()
            child_name = relation.get_relation_name()
            print "Do we need top projection in subquery ?"
            print "predicate.get_field_names()", predicate.get_field_names()
            print "parent_fields", parent_fields
            if not predicate.get_field_names() <= parent_fields:
                parent_fields |= predicate.get_field_names() # XXX jordan i don't understand this 
                require_top_projection = True 
            if not predicate.get_value_names() <= parent_fields:
                require_top_projection = True 
            child_fields[child_name] |= predicate.get_value_names()

        # 3) Optimize the main query (parent) and its subqueries (children)
        for i, child in enumerate(self.children[:]):
            relation = self.relations[i]
            child_name = relation.get_relation_name()
            self.children[i] = child.optimize_projection(child_fields[child_name])

        # Note:
        # if parent_fields < self.parent.get_query().get_select():
        # This is not working if the parent has fields not in the subquery:
        # eg. requested = slice_hrn, resource && parent = slice_hrn users

        if self.parent.get_query().get_select() - parent_fields: 
            #self.parent = self.parent.optimize_projection(parent_fields)
            self.parent = self.parent.optimize_projection(parent_fields.intersection(self.parent.get_query().get_select()))

        # 4) Some fields (used to connect the parent node to its child node) may be not
        # queried by the user. In this case, we ve to add a Projection
        # node which will filter those fields.
        if require_top_projection:
            print "fields", fields
            return Projection(self, fields) #jordan
        return self

