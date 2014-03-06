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

from manifold.core.destination      import Destination
from manifold.core.exceptions       import ManifoldInternalException
from manifold.core.filter           import Filter
from manifold.core.operator_slot    import ParentChildrenSlotMixin, PARENT
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Records
from manifold.core.relation         import Relation
from manifold.core.record           import Record
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.util.log              import Log
from manifold.util.misc             import dict_set, dict_append
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
        self._num_children_started = 0
        for producer, relation in child_producer_relation_list:
            relation_name = relation.get_relation_name()
            if not relation_name:
                raise ManifoldInternalException, "All children for SubQuery should have named relations"
            data = {
                'relation'  : relation,
                'packet'    : None,
                'done'      : False,
                'records'   : []
            }
            # NOTE: all relations in a SubQuery have names
            print "CHILDREN RELATION", relation
            self._set_child(producer, data, child_id = relation_name)
            self._num_children_started += 1

        # Member variables (could be stored in parent data)
        self.parent_output = []
        self._parent_done = False

        # Dictionary indexed by child_id, storing the packets that will be sent
        # to the different children.
        # see Subquery.split_packet()
        self._child_packets = None

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
    # Helpers
    #---------------------------------------------------------------------------

    def _set_child_done(self, child_id):
        self._update_child_data(child_id, lambda d: dict_set(d, 'done', True))
        self._num_children_started -= 1
        if self._num_children_started == 0:
            self._all_done()

    def _get_child_packet(self, child_id):
        return self._get_child_data(child_id).get('packet')

    def _set_child_packet(self, child_id, packet):
        self._update_child_data(child_id, lambda d: dict_set(d, 'packet', packet))

    def _update_child_packet(self, child_id, function):
        self._set_child_packet(child_id, function(packet)) # XXX Eventually more parameters

    def _add_child_record(self, child_id, record):
        self._update_child_data(child_id, lambda d: dict_append(d, 'records',
        record))

    def _get_child_records(self, child_id):
        return self._get_child_data(child_id).get('records')

    def split_destination(self, destination):
        """
        Returns:
            A tuple (parent, [child_queries])
        """

        # Prepare parent and child destinations
        parent_destination = Destination()
        child_destinations = {}
        for child_id, child, child_data in self._iter_children():
            child_destinations[child_id] = Destination()

        # Object
        parent_object = destination.get_object()
        parent_destination.set_object(parent_object)

        # Filter
        #
        # Until further work, all filters are applied to the parent, filters on
        # child fields should not reach the operator
        for predicate in destination.get_filter():
            # What about Filter.split_fields() ?
            key_field, key_subfield, op, value = predicate.get_tuple_ext()
            if key_subfield:
                raise ManifoldInternalException('Filters involving children should not reach SubQuery')
            parent_destination.add_filter(predicate)

        # Fields
        for field, subfield in destination.get_fields().iter_field_subfield():
            parent_destination.add_fields(field)
            if subfield:
                # NOTE : the field should be the identifier of the child
                # Remember that all relations involved in a SubQuery are named.
                child_destinations[field].add_fields(subfield)
        
        # Keys & child objects
        for child_id, child, child_data in self._iter_children():
            predicate = child_data.get('relation').get_predicate()
            parent_destination.add_fields(predicate.get_field_names())
            child_destinations[child_id].add_fields(predicate.get_value_names())

            child_object = self._get_child(child_id).get_destination().get_object()
            child_destinations[child_id].set_object(child_object)

        return (parent_destination, child_destinations)
    
    def merge_destination(self, parent_destination, children_destinations):
        """
        """
        # XXX Do we need the parameters ?

    def split_packet(self, packet):
        """
        Args:
            packet (Packet): the Packet to split among the operator's parent and
            children.

        Returns:
            A tuple formed of the parent Packet, and a dictionary of children
            Packet's indexed by their child_id.
        """
        child_packets = {}

        parent_destination, child_destinations = self.split_destination(packet.get_destination())

        parent_packet = packet.clone()
        parent_packet.set_destination(parent_destination)

        for child_id, child, child_data in self._iter_children():
            child_packet = packet.clone()
            child_packet.set_destination(child_destinations[child_id])
            child_packets[child_id] = child_packet

        return parent_packet, child_packets

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
            parent_packet, child_packets = self.split_packet(packet)
            for child_id, child_packet in child_packets.items():
                self._set_child_packet(child_id, child_packet)
            self.send_to(self._get_parent(), parent_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            source_id = self._get_source_child_id(packet)

            #if packet.get_source() == self._producers.get_parent_producer(): # XXX
            record = packet
            if source_id == PARENT: # if not self._parent_done:

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
                # NOTE: source_id is the child_id
                if not record.is_empty():
                    # Store the results for later...
                    self._add_child_record(source_id, record)

                if record.is_last():
                    self._set_child_done(source_id)

        else: # TYPE_ERROR
            self.forward_upstream(packet)

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
            self.forward_upstream(Record(last = True))
            return

        # XXX We should prevent this in ast.py
        if self._get_num_children() == 0:
            # The top operator has build a SubQuery node without child queries,
            # so this SubQuery operator is useless and should be replaced by
            # its main query.
            Log.warning("SubQuery::run_children: no child node. The query plan could be improved")
            self.forward_upstream(Record(last = True))
            return

        for child_id, child, child_data in self._iter_children():
            predicate = child_data.get('relation').get_predicate()
            child_packet = child_data.get('packet')

            child_records = Records()
            for parent_record in self.parent_output:
                _child_records = parent_record.get(child_id)
                for _child_record in _child_records:
                    child_key = predicate.get_field_names()
                    child_fields = set(_child_record.keys())
                    for field in child_key - child_fields:
                        _child_record[field] = parent_record.get(field)
                    # ADD KEY IF MISSING
                    child_records.add_record(Record(_child_record))

            # XXX We need to inject something in the child packet
            # It's possible that those child packets miss the full key

            child_packet.set_records(child_records)

            print "CHILD PACKET", child_packet
            print "CHILD RECORDS", child_records
            self.send_to(child, child_packet)

#DEPRECATED|        # Inspect the first parent record to deduce which fields have already
#DEPRECATED|        # been fetched 
#DEPRECATED|        first_record = self.parent_output[0]
#DEPRECATED|        parent_fields = set(first_record.keys())
#DEPRECATED|
#DEPRECATED|        # Optimize child queries according to the fields already retrieved thanks
#DEPRECATED|        # to the parent query.
#DEPRECATED|        useless_children = set()
#DEPRECATED|
#DEPRECATED|        def handle_child(child_producer, child_data):
#DEPRECATED|            print "producer", child_producer
#DEPRECATED|            print "data", child_data
#DEPRECATED|            relation = child_data.get('relation', None)
#DEPRECATED|            # Test whether the current child provides relevant fields (e.g.
#DEPRECATED|            # fields not already fetched in the parent record). If so, reduce
#DEPRECATED|            # the set of queried field in order to only retrieve relevant fields.
#DEPRECATED|            child_fields = child_producer.get_destination().get_fields()
#DEPRECATED|            relation_name = relation.get_relation_name()
#DEPRECATED|            already_fetched_fields = set()
#DEPRECATED|            if relation_name in parent_fields:
#DEPRECATED|                subrecord = first_record.get(relation_name, None)
#DEPRECATED|                if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
#DEPRECATED|                    already_fetched_fields = set(subrecord[0].keys()) if subrecord else set()
#DEPRECATED|                else:
#DEPRECATED|                    already_fetched_fields = set(subrecord.keys())
#DEPRECATED|
#DEPRECATED|            # we need to keep key used for subquery
#DEPRECATED|            relevant_fields = child_fields - already_fetched_fields
#DEPRECATED|
#DEPRECATED|            if not relevant_fields:
#DEPRECATED|                useless_children.add(child_producer)
#DEPRECATED|                return child_producer
#DEPRECATED|
#DEPRECATED|            elif child_fields != relevant_fields:
#DEPRECATED|                # Note we need to keep key
#DEPRECATED|                key_fields = relation.get_predicate().get_value_names()
#DEPRECATED|                return child_producer.optimize_projection(relevant_fields | key_fields)
#DEPRECATED|
#DEPRECATED|        self._update_children_producers(handle_child)
#DEPRECATED|
#DEPRECATED|        # If every children are useless, this means that we already have full records
#DEPRECATED|        # thanks to the parent query, so we simply forward those records.
#DEPRECATED|        if self._get_num_children() == len(useless_children):
#DEPRECATED|            map(self.forward_upstream, self.parent_output)
#DEPRECATED|            self.forward_upstream(Record(last = True))
#DEPRECATED|            return
#DEPRECATED|
#DEPRECATED|        # Loop through children and inject the appropriate parent results
#DEPRECATED|        def handle_child(child_producer, child_data):
#DEPRECATED|            relation = child_data.get('relation')
#DEPRECATED|            if child_producer in useless_children:
#DEPRECATED|                return child_producer
#DEPRECATED|
#DEPRECATED|            # We have two cases:
#DEPRECATED|            # (1) either the parent query has subquery fields (a list of child
#DEPRECATED|            #     ids + eventually some additional information)
#DEPRECATED|            # (2) either the child has a backreference to the parent
#DEPRECATED|            #     ... eventually a partial reference in case of a 1..N relationship
#DEPRECATED|            #
#DEPRECATED|            # In all cases, we will collect all identifiers to proceed to a
#DEPRECATED|            # single child query for efficiency purposes, unless it's not
#DEPRECATED|            # possible (?).
#DEPRECATED|            #
#DEPRECATED|            # We have several parent records stored in self.parent_output
#DEPRECATED|            #
#DEPRECATED|            # /!\ Can we have a mix of (1) and (2) ? For now, let's suppose NO.
#DEPRECATED|            #  *  We could expect key information to be stored in the DBGraph
#DEPRECATED|
#DEPRECATED|            # The operation to be performed is understood only be looking at the predicate
#DEPRECATED|            predicate = relation.get_predicate()
#DEPRECATED|
#DEPRECATED|            key, op, value = predicate.get_tuple()
#DEPRECATED|            if op == eq:
#DEPRECATED|                # 1..N
#DEPRECATED|                # Example: parent has slice_hrn, resource has a reference to slice
#DEPRECATED|                if relation.get_type() == Relation.types.LINK_1N_BACKWARDS:
#DEPRECATED|                    parent_ids = [record[key] for record in self.parent_output]
#DEPRECATED|                    if len(parent_ids) == 1:
#DEPRECATED|                        parent_id, = parent_ids
#DEPRECATED|                        filter_pred = Predicate(value, eq, parent_id)
#DEPRECATED|                    else:
#DEPRECATED|                        filter_pred = Predicate(value, included, parent_ids)
#DEPRECATED|                else:
#DEPRECATED|                    parent_ids = []
#DEPRECATED|                    for parent_record in self.parent_output:
#DEPRECATED|                        record = parent_record.get_value(key)
#DEPRECATED|                        if not record:
#DEPRECATED|                            record = []
#DEPRECATED|                        if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
#DEPRECATED|                            # we have a list of elements 
#DEPRECATED|                            # element = id or dict    : clé simple
#DEPRECATED|                            #         = tuple or dict : clé multiple
#DEPRECATED|                            parent_ids.extend([self.get_element_key(r, value) for r in record])
#DEPRECATED|                        else:
#DEPRECATED|                            parent_ids.append(self.get_element_key(record, value))
#DEPRECATED|                        
#DEPRECATED|                    #if isinstance(key, tuple):
#DEPRECATED|                    #    parent_ids = [x for record in self.parent_output if key in record for x in record[key]]
#DEPRECATED|                    #else:
#DEPRECATED|                    #    ##### record[key] = text, dict, or list of (text, dict) 
#DEPRECATED|                    #    parent_ids = [record[key] for record in self.parent_output if key in record]
#DEPRECATED|                    #    
#DEPRECATED|                    #if parent_ids and isinstance(parent_ids[0], dict):
#DEPRECATED|                    #    parent_ids = map(lambda x: x[value], parent_ids)
#DEPRECATED|
#DEPRECATED|                    if len(parent_ids) == 1:
#DEPRECATED|                        parent_id, = parent_ids
#DEPRECATED|                        filter_pred = Predicate(value, eq, parent_id)
#DEPRECATED|                    else:
#DEPRECATED|                        filter_pred = Predicate(value, included, parent_ids)
#DEPRECATED|
#DEPRECATED|                return child_producer.optimize_selection(Filter().filter_by(filter_pred))
#DEPRECATED|
#DEPRECATED|            elif op == contains:
#DEPRECATED|                raise Exception, "TBD"
#DEPRECATED|                # 1..N
#DEPRECATED|                # Example: parent 'slice' has a list of 'user' keys == user_hrn
#DEPRECATED|                for slice in self.parent_output:
#DEPRECATED|                    if not child.get_query().object in slice: continue
#DEPRECATED|                    users = slice[key]
#DEPRECATED|                    # users est soit une liste d'id, soit une liste de records
#DEPRECATED|                    user_data = []
#DEPRECATED|                    for user in users:
#DEPRECATED|                        if isinstance(user, dict):
#DEPRECATED|                            user_data.append(user)
#DEPRECATED|                        else:
#DEPRECATED|                            # have have a key
#DEPRECATED|                            # XXX Take multiple keys into account
#DEPRECATED|                            user_data.append({value: user}) 
#DEPRECATED|                    # Let's inject user_data in the right child
#DEPRECATED|                    child.inject(user_data, value, None) 
#DEPRECATED|
#DEPRECATED|            else:
#DEPRECATED|                raise Exception, "No link between parent and child queries"
#DEPRECATED|
#DEPRECATED|        self._update_children_producers(handle_child)

    def _all_done(self):
        """
        \brief Called when all children of the current subquery are done: we
         process results stored in the parent.
        """

        try:
            for parent_record in self.parent_output:
                # Dispatching child results
                for child_id, child, child_data in self._iter_children():

                    relation  = child_data.get('relation')
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
                        for child_record in self._get_child_records(child_id):
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
                                # Find the corresponding record in child_records and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter().filter_by(Predicate(value, eq, record[value]))
                                    for r in self._get_child_records(child_id):
                                        if filter.match(r):
                                            record.update(r)
                        else:
                            for record in parent_record[child_object]:
                                # Find the corresponding record in child_records and update the one in the parent with it
                                for k, v in record.items():
                                    filter = Filter()
                                    for field in value:
                                        filter = filter.filter_by(Predicate(field, eq, record[field]))
                                    for r in self._get_child_records(child_id):
                                        if filter.match(r):
                                            record.update(r)
                        
                    else:
                        raise Exception, "No link between parent and child queries"

                self.forward_upstream(parent_record)
            self.forward_upstream(Record(last = True))
        except Exception, e:
            print "EEE", e
            traceback.print_exc()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator. 
        """
        dparent = self._get_parent().get_destination()
        children_destination_relation_list = []
        for child, child_data in self._get_children(get_data = True):
            children_destination_relation_list.append(
                (child.get_destination(), child_data.get('relation'))
            )
        return dparent.subquery(children_destination_relation_list)

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


