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
from manifold.core.operator_slot    import ParentChildrenSlotMixin
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

#------------------------------------------------------------------
# SUBQUERY node
#------------------------------------------------------------------

class SubQuery(Operator, ParentChildrenSlotMixin):
    """
    SUBQUERY operator (cf nested SELECT statements in SQL)
        self.parent represents the main query involved in the SUBQUERY operation.
        self.children represents each subqueries involved in the SUBQUERY operation.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, parent_producer, child_producer_relation_list):
        """
        Constructor
        Args:
            parent: The main query (AST instance ?)
            children_ast_relation_list: A list of (AST , Relation) tuples
        """
            
        # Initialization (passing a tuple as producers stores the second parameter as data)
        Operator.__init__(self)
        ParentChildrenSlotMixin.__init__(self)

        self._set_parent(parent_producer)
        for producer, relation in child_producer_relation_list:
            data = {
                'relation'  : relation,
                'done'      : False,
                'results'   : []
            }
            self._set_child(producer, data)

        # Member variables (could be stored in parent data)
        self.parent_output = []
        self._parent_done = False

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this SubQuery Operator.
        """
        return '<subqueries>'

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

            self._get_parent().send(parent_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            child_id = self._get_source_child_id(packet)

            #if packet.get_source() == self._producers.get_parent_producer(): # XXX
            record = packet
            if not self._parent_done:

                # Store the record for later...
                if not record.is_empty():
                    self.parent_output.append(record)

                # formerly parent_callback
                if record.is_last():
                    # When we have received all parent records, we can run children
                    self._parent_done = True
                    if self.parent_output:
                        self._run_children()
                    return

            else:
                # formerly child callback
                if record.is_last():
                    self.status.completed(child_id)
                    return
                # Store the results for later...
                self._update_child_data(child_id, lambda d: d.append(record))

        else: # TYPE_ERROR
            self.send(packet)

    @staticmethod
    def get_element_key(element, key):
        if isinstance(element, dict):
            # record
            return element.get_value(key)
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

        # XXX We should prevent this in ast.py
        if self._get_num_children() == 0:
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

        def handle_child(child_producer, child_data):
            relation = child_data.get('relation', None)
            # Test whether the current child provides relevant fields (e.g.
            # fields not already fetched in the parent record). If so, reduce
            # the set of queried field in order to only retrieve relevant fields.
            child_fields = child_producer.get_destination().get_fields()
            relation_name = relation.get_relation_name()
            already_fetched_fields = set()
            if relation_name in parent_fields:
                subrecord = first_record.get(relation_name, None)
                if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
                    already_fetched_fields = set(subrecord[0].keys()) if subrecord else set()
                else:
                    already_fetched_fields = set(subrecord.keys())

            # we need to keep key used for subquery
            relevant_fields = child_fields - already_fetched_fields

            if not relevant_fields:
                useless_children.add(child_producer)
                return child_producer

            elif child_fields != relevant_fields:
                # Note we need to keep key
                key_fields = relation.get_predicate().get_value_names()
                return child_producer.optimize_projection(relevant_fields | key_fields)

        self._update_children_producers(handle_child)

        # If every children are useless, this means that we already have full records
        # thanks to the parent query, so we simply forward those records.
        if self._get_num_children() == len(useless_children):
            map(self.send, self.parent_output)
            self.send(Record(last = True))
            return

        # Loop through children and inject the appropriate parent results
        def handle_child(child_producer, child_data):
            relation = child_data.get('relation')
            if child_producer in useless_children:
                return child_producer

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
                        record = parent_record.get_value(key)
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

                return child_producer.optimize_selection(Filter().filter_by(filter_pred))

            elif op == contains:
                raise Exception, "TBD"
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

        self._update_children_producers(handle_child)

        #print "*** before run children ***"
        #self.dump()

        # We make another loop since the children might have been modified in
        # the previous one.
        for child_id, child_producer, _ in self._iter_children():
            if not child_producer in useless_children:
                self.status.started(child_id)
        for child_id, child_producer, _ in self._iter_children():
            if not child_producer in useless_children:
                pass
                # child_producer.receive(child_query)

    def _all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """
        try:
            for parent_record in self.parent_output:
                # Dispatching child results
                for child_id, child_producer, relation in self._iter_children():

                    predicate = relation.get_predicate()

                    key, op, value = predicate.get_tuple()
                    
                    if op == eq:
                        # 1..N
                        # Example: parent has slice_hrn, resource has a reference to slice
                        #            PARENT       CHILD
                        # Predicate: (slice_hrn,) == slice

                        # Collect in parent all child such as they have a pointer to the parent
                        record = parent_record.get_value(key)
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
                        child_object = child_producer.get_query().get_object()
                        if parent_record[child_object]:
                            record = parent_record[child_object][0]
                            if not isinstance(record, dict):
                                parent_record[child_object] = [{value: record} for record in parent_record[child_object]]

                        if isinstance(value, StringTypes):
                            for record in parent_record[child_object]:
                                # Find the corresponding record in child_results and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter().filter_by(Predicate(value, eq, record[value]))
                                    for r in self.child_results[i]:
                                        if filter.match(r):
                                            record.update(r)
                        else:
                            for record in parent_record[child_object]:
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
        Log.debug("TODO SubQuery::optimize_selection")
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
        parent_fields = set([field for field in fields if not "." in field]) \
            | set([field.split('.')[0] for field in fields if "." in field])


        # XXX We need data associated with each producer
        def handle_child(child_producer, child_data):
            relation = child_data.get('relation')

            parent_fields = set([field for field in fields if not "." in field]) \
                | set([field.split('.')[0] for field in fields if "." in field])

            # 1) If the SELECT clause refers to "a.b", this is a Query related to the
            # child subquery related to "a". If the SELECT clause refers to "b" this
            # is always related to the parent query.
            predicate    = relation.get_predicate()
            child_name   = relation.get_relation_name()
            child_fields = set([field.split('.', 1)[1] for field in fields if field.startswith("%s." % child_name)])

            # 2) Add to child_fields and parent_fields the field names needed to
            # connect the parent to its children. If such fields are added, we will
            # filter them in step (4). Once we have deduced for each child its
            # queried fields (see (1)) and the fields needed to connect it to the
            # parent query (2), we can start the to optimize the projection (3).
            if not predicate.get_value_names() <= parent_fields:
                require_top_projection = True 
            child_fields |= predicate.get_value_names()

            # 3) Optimize the main query (parent) and its subqueries (children)
            return child_producer.optimize_projection(child_fields)

        self._update_children_producers(handle_child)

        require_top_projection = False
        parent_fields = set([field for field in fields if not "." in field]) \
            | set([field.split('.')[0] for field in fields if "." in field])
        for _, _, data in self._iter_children():
            relation = data.get('relation', None)
            predicate = relation.get_predicate()
            if not predicate.get_field_names() <= parent_fields:
                parent_fields |= predicate.get_field_names() # XXX jordan i don't understand this 
                require_top_projection = True 

        # Note:
        # if parent_fields < self.parent.get_query().get_select():
        # This is not working if the parent has fields not in the subquery:
        # eg. requested = slice_hrn, resource && parent = slice_hrn users
        real_parent_fields = self._get_parent().get_destination().get_fields()
        if real_parent_fields - parent_fields:
            opt_parent_fields = parent_fields.intersection(real_parent_fields)
            self._update_parent_producer(lambda p, d: p.optimize_projection(opt_parent_fields))

        # 4) Some fields (used to connect the parent node to its child node) may be not
        # queried by the user. In this case, we ve to add a Projection
        # node which will filter those fields.
        if require_top_projection:
            return Projection(self, fields) #jordan
        return self


