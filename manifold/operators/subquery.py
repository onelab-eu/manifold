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
from manifold.core.field_names      import FieldNames, FIELD_SEPARATOR
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
from manifold.util.misc             import dict_set, dict_append, is_sublist
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
    # Constructors
    #---------------------------------------------------------------------------

    def __init__(self, parent_producer, child_producer_relation_list):
        """
        Constructor
        Args:
            parent: The parent Operator instance.
            child_producer_relation_list: A list of (Producer, Relation) tuples
                where each tuple correspond to a sub query.
        """
            
        # Initialization (passing a tuple as producers stores the second parameter as data)
        Operator.__init__(self)
        ParentChildrenSlotMixin.__init__(self)
        self._set_parent(parent_producer)
        self._num_children_started = 0
        for producer, relation in child_producer_relation_list:
            self._add_child(producer, relation)

        # Member variables (could be stored in parent data)
        self.parent_output = Records()
        self._parent_done = False

        # Dictionary indexed by child_id, storing the packets that will be sent
        # to the different children.
        # see Subquery.split_packet()
        self._child_packets = None

        # LOCAL ?
        if len(child_producer_relation_list) == 0:
            self._local = False
        else:
            child, relation = child_producer_relation_list[0]
            self._local = relation.is_local()

    def copy(self):
        new_parent = self._get_parent().copy()
        new_child_producer_relation_list = list()
        for child_id, child, child_data in self._iter_children():
            relation = child_data.get('relation')
            new_producer = child.copy()
            new_relation = relation.copy()
            new_producer_relation = (new_producer, new_relation)
            new_child_producer_relation_list.append(new_producer_relation)
        return SubQuery(new_parent, new_child_producer_relation_list)

    #---------------------------------------------------------------------------
    # Class methods
    #---------------------------------------------------------------------------

    @classmethod
    def _make(cls, parent, child, relation):
        """
        Internal usage.
        We have decided to build the subquery operator (after checking for
        local status), we now need to decide whether to insert a cross product
        or not.
        """
        if False: #parent.capabilities.is_onjoin(): #We need more than the test for ONJOIN.. We might have all the needed parameters, because the user expressed them, or from a parent operator
            # We need to have all root_key_field_names available before running the
            # onjoin query
            root_key_field_names = None # parent.keys.one().get_field_names()

            # We assume for now that if the field_names will be either available as a
            # WHERE condition (not available at this stage), or through a
            # subquery. We only look into subqueries, if the field is not
            # present, that means it will be there at execution time. It will
            # fail otherwise.
            #
            # A = onjoin(X,Y)
            #                   
            # SQ = A
            #   |_ X     <=>  SQ  - |X| - x - X
            #   |_ Y              \     \   \
            #   |_ Z                Z     A   Y
            #
            # We will have:
            #    |X| if a root key field is given by a subquery == not available #    in a filter
            #     x  if more than 1 subquery is involved
            # 
            # NOTE: x will either return a single record with a list of values,
            # or multiple records with simple values

            xp_ast_relation, sq_ast_relation = [], []
            xp_key = ()
            xp_value = ()

# ??? #            # We go though the set of subqueries to find
# ??? #            # - the ones for the cartesian product
# ??? #            # - the ones for the subqueries
# ??? #            for name, ast_relation in self.subqueries.items():
# ??? #                if name in root_key_field_names:
# ??? #                    ast, relation = ast_relation
# ??? #                    key, _, value = relation.get_predicate().get_tuple()
# ??? #                    assert isinstance(key, StringTypes), "Invalid key"
# ??? #                    assert not isinstance(value, tuple), "Invalid value"
# ??? #                    xp_key   += (value,)
# ??? #                    xp_value += (key,)
# ??? #                    xp_ast_relation.append(ast_relation)
# ??? #                else:
# ??? #                    sq_ast_relation.append(ast_relation)

#DEPRECATED|             ast = self.ast
#DEPRECATED| 
#DEPRECATED|             # SUBQUERY
#DEPRECATED|             if sq_ast_relation:
#DEPRECATED|                 ast.subquery(sq_ast_relation)
#DEPRECATED| 
#DEPRECATED|             # CARTESIAN PRODUCT
#DEPRECATED|             query = Query.action('get', self.root.get_name()).select(set(xp_key))
#DEPRECATED|             self.ast = AST(self._router).cartesian_product(xp_ast_relation, query)
#DEPRECATED| 
#DEPRECATED|             # JOIN
#DEPRECATED|             predicate = Predicate(xp_key, eq, xp_value)
#DEPRECATED|             self.ast.left_join(ast, predicate)

            # XXX TODO !!!
            # 1) Cross product
            if len(xp_ast_relation) > 1:
                pass

            # 2) Left join
            if len(xp_ast_relation) > 0:
                pass
                
            # 3) Subquery
# ??? #            if len(sq_ast_relation) > 0:
# ??? #                self.ast.subquery
# ??? #                pass
        else:
            return cls(parent, [(child, relation)])

    @classmethod
    def make(cls, parent, child, relation):
        """
        Default construction for a SubQuery operator, handling special cases
        such as local queries.
        """
        if relation.is_local():
            # here we need to be careful that no Rename of LeftJoin operator is
            # present in the child. If so, let's pop them out of the child, with
            # the appropriate treatment.
            return cls.extract_from_local_child(parent, child, relation, next_operator = None)
        else:
            return cls._make(parent, child, relation)

# MANDO: this function uses both cls and self!
# MANDO: you can't call is_local inside a classmethod
#MANDO|    @classmethod
#MANDO|    def add_child(cls, parent, child, relation):
#MANDO|        # assert: we only add local relations to a local subquery
#MANDO|        # assert: we cannot add children if the operator has already been started 
#MANDO|
#MANDO|        if self.is_local():
#MANDO|            return cls.extract_from_local_child(None, child, relation, next_operator = parent)
#MANDO|        return parent._add_child(child, relation)

# MANDO: you can't call is_local inside a classmethod
#MANDO|    @classmethod
    def add_child(self, parent, child, relation):
        # assert: we only add local relations to a local subquery
        # assert: we cannot add children if the operator has already been started 

        if self.is_local():
            return self.extract_from_local_child(None, child, relation, next_operator = parent)
        return parent._add_child(child, relation)


    def _add_child(self, child, relation):
        relation_name = relation.get_relation_name()
        if not relation_name:
            raise ManifoldInternalException, "All children for SubQuery should have named relations"
        data = {
            'relation'  : relation,
            'packet'    : None,
            'done'      : False,
            'records'   : Records()
        }
        # NOTE: all relations in a SubQuery have names
        self._set_child(child, data, child_id = relation_name)
        self._num_children_started += 1

        return self

    @classmethod
    def extract_from_local_child(cls, parent, child, relation, next_operator = None):
        """
        """
        # XXX This should be done at another moment to avoid a UNION of two AST
        # with the same RENAME and LEFT JOIN

        # To avoid cyclic dependencies
        from manifold.operators.left_join   import LeftJoin
        from manifold.operators.rename      import Rename
        from manifold.operators.right_join  import RightJoin

        # (o)--[SQ]--[PARENT]
        #          \
        #           \--(o)  (o)--[XX]--[XX]--(o) (o)--[NEW_CHILD]
        #                    ^
        #                  child
        #                  = top
        top = child
        prev = None
        bottom = child

        while True:
            # If the current operator has to be migrated...
            name = relation.get_name()
            if isinstance(bottom, LeftJoin):
                # Update the predicate
                predicate = bottom.get_predicate()
                predicate.update_key(lambda k: FieldNames.join(name, k))

                # Get following operator
                prev = bottom
                bottom = bottom._get_left()

            elif isinstance(bottom, Rename):
                # Update names
                bottom.update_aliases(lambda k, v: (FieldNames.join(name, k), FieldNames.join(name, v)))

                # Get following operator
                prev = bottom
                bottom = bottom._get_child()

            else:
                break

        # Plug the children inside a new/an existing SQ
        if not next_operator:
            # Create subquery
            next_operator = cls._make(parent, bottom, relation)
        else:
            next_operator._add_child(bottom, relation)
                
        if top == bottom:
            # Nothing has to be migrated
            return next_operator
        else:
            # top -- ... -- bottom is the new root of SQ
            if isinstance(prev, LeftJoin):
                prev._set_left(next_operator)
            elif isinstance(prev, Rename):
                prev._set_child(next_operator)
            else:
                raise NotImplemented
            return top

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def is_local(self):
        return self._local

    def set_local(self):
        self._local = True

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this SubQuery Operator.
        """
        return '<subqueries%s>' % (' local' if self.is_local() else '')

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
        # child field_names should not reach the operator
        for predicate in destination.get_filter():
            # What about Filter.split_field_names() ?
            key_field, key_subfield, op, value = predicate.get_tuple_ext()
            if key_subfield:
                #raise ManifoldInternalException('Filters involving children should not reach SubQuery')
                #destination.remove_filter(predicate)
                Log.warning('Filters involving children should not reach SubQuery')
            parent_destination.add_filter(predicate)

        # FieldNames

        for field, subfield in destination.get_field_names().iter_field_subfield():
            # XXX THIS DOES NOT TAKE SHORTCUTS INTO ACCOUNT
            parent_destination.add_field_names(field)
            if subfield:
                # NOTE : the field should be the identifier of the child
                # Remember that all relations involved in a SubQuery are named.
                child_destinations[field].add_field_names(subfield)

#MARCO|        parent_field_names = self._get_parent().get_destination().get_field_names()
#MARCO|        parent_destination.add_field_names(destination.get_field_names() & parent_field_names)
#MARCO|        for child_id, child, child_data in self._iter_children():

            child_field_names = FieldNames()
            for field in child.get_destination().get_field_names():
                child_field_names.add(FieldNames.join(child_id, field))
            child_destinations[child_id].add_field_names(destination.get_field_names() & child_field_names)

#MARCO|            child_provided_field_names = child.get_destination().get_field_names()
#MARCO|
#MARCO|            child_field_names = FieldNames()
#MARCO|            for f in destination.get_field_names():
#MARCO|                for cf in child_provided_field_names:
#MARCO|                    # f.split(FIELD_SEPARATOR)[1:]      # (hops) ttl    (hops) ip
#MARCO|                    # cf.split(FIELD_SEPARATOR)         # ttl           probes ip 
#MARCO|                    f_arr = f.split(FIELD_SEPARATOR)
#MARCO|                    if len(f_arr) > 1 and f_arr[0] == child_id:
#MARCO|                        f_arr = f_arr[1:]
#MARCO|                    cf_arr = cf.split(FIELD_SEPARATOR)
#MARCO|                    flag, shortcut = is_sublist(f_arr, cf_arr)
#MARCO|                    if f_arr and flag:
#MARCO|                        child_field_names.add(cf)
#MARCO|
#MARCO|            child_destinations[child_id].add_field_names(child_field_names)
        
        # Keys & child objects
        for child_id, child, child_data in self._iter_children():
            predicate = child_data.get('relation').get_predicate()
            parent_destination.add_field_names(predicate.get_field_names())
            # XXX HOSTILE In fact this is not necessary for local keys... But how will
            # we assembles subrecords ??????
            child_destinations[child_id].add_field_names(predicate.get_value_names())

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

#DEPRECATED|    def split_record(self, record):
#DEPRECATED|        method_subrecords = dict()
#DEPRECATED|        for k, v in record.items():
#DEPRECATED|            if isinstance(v, Records):
#DEPRECATED|                method_subrecords[k] = v
#DEPRECATED|            elif isinstance(v, list): # XXX Temporary
#DEPRECATED|                method_subrecords[k] = Records(v)
#DEPRECATED|
#DEPRECATED|        if not method_subrecords:
#DEPRECATED|            return
#DEPRECATED|
#DEPRECATED|        uuid = record.get_uuid()
#DEPRECATED|        for method, _subrecords in method_subrecords.items():
#DEPRECATED|            subrecords = list()
#DEPRECATED|            for subrecord in _subrecords:
#DEPRECATED|                subrecord = self.split_record(subrecord)
#DEPRECATED|                subrecord.set_parent_uuid(uuid)
#DEPRECATED|                subrecords.append(subrecord)
#DEPRECATED|            self._router.add_to_local_cache(method, uuid, subrecords)
#DEPRECATED|        return record

    def receive_impl(self, packet):
        """
        Handle an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """

        # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
        # We cannot treat all packets the same until we figure out how to merge
        # two packets (JOIN and SUBQUERY)
        # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
        if packet.get_protocol() in Packet.PROTOCOL_QUERY:
            parent_packet, child_packets = self.split_packet(packet)
            for child_id, child_packet in child_packets.items():
                self._set_child_packet(child_id, child_packet)
            self.send_to(self._get_parent(), parent_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_CREATE:
            # XXX Here we want to know which child has sent the packet...
            source_id = self._get_source_child_id(packet)
            record = packet
            is_last = record.is_last()
            record.unset_last()

#DEPRECATED|            # We will extract subrecords from the packet and store them in the
#DEPRECATED|            # local cache
#DEPRECATED|            # XXX How can we easily spot subrecords
#DEPRECATED|            # XXX This should disappear
#DEPRECATED|            record, subrecord_dict = self.split_record(record)

            # XXX For local subqueries we do not even need to wonder, only
            # parent answers
            assert source_id is not None

            #if packet.get_source() == self._producers.get_parent_producer(): # XXX
            if source_id == PARENT: # if not self._parent_done:
                # Store the record for later...

                if not record.is_empty():
                    self.parent_output.append(record)

                # formerly parent_callback
                if is_last:
                    # When we have received all parent records, we can run children
                    self._parent_done = True
                    if self.parent_output:
                        self._run_children()
                    else:
                        self.forward_upstream(Record(last = True))
                    return

            else:
                print "PACKET", packet
                # NOTE: source_id is the child_id
                if not record.is_empty():
                    # Store the results for later...
                    self._add_child_record(source_id, record)

                if is_last:
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
            map(self.forward_upstream, self.parent_output)
            self.forward_upstream(Record(last = True))
            return

        if self.is_local():
            map(self.forward_upstream, self.parent_output)
            self.forward_upstream(Record(last = True))
            return

        # We look at every children
        for child_id, child, child_data in self._iter_children():
            print "_run_children RELATION=", child_data.get('relation')
            predicate = child_data.get('relation').get_predicate()
            child_packet = child_data.get('packet')
            child_key = predicate.get_value_names()

            self.send_to(child, child_packet)

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

                    # XXX HOW DO WE MANAGE LOCAL QUERIES HERE ?
                    print "SEARCHING FOR LOCAL QUERIES"
                    print "  . relation", relation
                    print "  . predicate", predicate
                    
                    # The following code is messy... although it is in charge of
                    # mapping the subrecords with their parent records...
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

                        parent_record[relation.get_relation_name()] = Records()
                        for child_record in self._get_child_records(child_id):
                            if filter.match(child_record):
                                # 
                                # parent_record[relation.get_relation_name()]
                                # contains a list of children objects, with some
                                # properties
                                # their key is relation.get_value_names()
                                parent_record[relation.get_relation_name()].add_record(child_record)
                                

                    elif op == contains:
                        # 1..N
                        # Example: parent 'slice' has a list of 'user' keys == user_hrn
                        #            PARENT        CHILD
                        # Predicate: user contains (user_hrn, )

                        # first, replace records by dictionaries. This only works for non-composite keys
                        child_object = child_producer.get_destination().get_object()
                        if parent_record[child_object]:
                            record = parent_record[child_object].get_one()
                            if not isinstance(record, (dict, Record)):
                                parent_record[child_object] = [{value: record} for record in parent_record[child_object]]
                            if isinstance(record, dict):
                                raise Exception, "DEPRECATED"

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
            print "Exception subquery", e
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
        if self.is_local():
            # Don't propagate the selection
            #if filter <= self.get_destination().get_filter():
            return Selection(self, filter)
            #else:
            #    return self

        parent_filter, top_filter = Filter(), Filter()
        for predicate in filter:
            # XXX We need to know all field_names, and not only those that have been # asked !!!
            if predicate.get_field_names() <= self._get_parent().get_destination().get_field_names():
                parent_filter.add(predicate)
            else:
                Log.warning("SubQuery::optimize_selection() is only partially implemented : %r" % predicate)
                top_filter.add(predicate)

        if parent_filter:
            self._update_parent_producer(lambda p,d : p.optimize_selection(parent_filter))

        if top_filter:
            return Selection(self, top_filter)

        return self

    def optimize_projection(self, field_names):
        """
        Propagates projection (SELECT) through a SubQuery node.
        A field is relevant if:
            - it is explicitely queried (i.e. it is a field involved in the projection)
            - it is needed to perform the SubQuery (i.e. it is involved in a Predicate)
        Args:
            field_names: A frozenset of String containing the field_names involved in the projection.
        Returns:
            The optimized AST once this projection has been propagated.
        """

        if self.is_local():
            # Don't propagate the projection
            if field_names <= self.get_destination().get_field_names():
                return Projection(self, field_names)
            else:
                return self


        parent_field_names = FieldNames([field for field in field_names if not "." in field]) \
            | FieldNames([field.split('.')[0] for field in field_names if "." in field])

        # XXX We need data associated with each producer
        def handle_child(child_producer, child_data):
            relation = child_data.get('relation')

            parent_field_names = FieldNames([field for field in field_names if not "." in field]) \
                | FieldNames([field.split('.')[0] for field in field_names if "." in field])

            # 1) If the SELECT clause refers to "a.b", this is a Query related to the
            # child subquery related to "a". If the SELECT clause refers to "b" this
            # is always related to the parent query.
            predicate    = relation.get_predicate()
            child_name   = relation.get_relation_name()
            # XXX we have a method for this ?
            # XXX THIS IS WRONG IN CASE OF SHORTCUTS

            # requested : hops.ip
            # in child hop: on a ttl et probes.ip

            child_provided_field_names = child_producer.get_destination().get_field_names() 

            child_field_names = FieldNames()
            for f in field_names:
                for cf in child_provided_field_names:
                    # f.split(FIELD_SEPARATOR)[1:]      # (hops) ttl    (hops) ip
                    # cf.split(FIELD_SEPARATOR)         # ttl           probes ip 
                    f_arr = f.split(FIELD_SEPARATOR)
                    if len(f_arr) > 1 and f_arr[0] == child_name:
                        f_arr = f_arr[1:]
                    cf_arr = cf.split(FIELD_SEPARATOR)
                    flag, shortcut = is_sublist(f_arr, cf_arr)
                    if f_arr and flag:
                        child_field_names.add(cf)

            # XXX For the child, we will search for field_names that are not in the
            # parent, we also suppose there are no conflicts... this should have
            # been solved during the query plan phase...

            # 2) Add to child_field_names and parent_field_names the field names needed to
            # connect the parent to its children. If such field_names are added, we will
            # filter them in step (4). Once we have deduced for each child its
            # queried field_names (see (1)) and the field_names needed to connect it to the
            # parent query (2), we can start the to optimize the projection (3).
            if not predicate.get_value_names() <= parent_field_names:
                require_top_projection = True 
            child_field_names |= predicate.get_value_names()

            # 3) Optimize the main query (parent) and its subqueries (children)
            return child_producer.optimize_projection(child_field_names)

        self._update_children_producers(handle_child)

        require_top_projection = False
        parent_field_names = FieldNames([field for field in field_names if not "." in field]) \
            | FieldNames([field.split('.')[0] for field in field_names if "." in field])
        for _, _, data in self._iter_children():
            relation = data.get('relation', None)
            predicate = relation.get_predicate()
            if not predicate.get_field_names() <= parent_field_names:
                parent_field_names |= predicate.get_field_names() # XXX jordan i don't understand this 
                require_top_projection = True 

        # Note:
        # if parent_field_names < self.parent.get_query().get_select():
        # This is not working if the parent has field_names not in the subquery:
        # eg. requested = slice_hrn, resource && parent = slice_hrn users
        real_parent_field_names = self._get_parent().get_destination().get_field_names()
        if real_parent_field_names - parent_field_names:
            opt_parent_field_names = parent_field_names & real_parent_field_names
            self._update_parent_producer(lambda p, d: p.optimize_projection(opt_parent_field_names))

        # 4) Some field_names (used to connect the parent node to its child node) may be not
        # queried by the user. In this case, we ve to add a Projection
        # node which will filter those field_names.
        if require_top_projection:
            return Projection(self, field_names) #jordan
        return self

    #---------------------------------------------------------------------------
    # Algebraic rules
    #---------------------------------------------------------------------------

    def subquery(self, ast, relation):
        """
        SQ_new o SQ

        Overrides the default behaviour where the SQ operator is added at the
        top.
        """
        if relation.is_local():
            if self.is_local():
                # Simple behaviour : since all children can be executed in parallel,
                # let's add the child ast as a new children of the current SQ node.
                return self.add_child(ast, relation)
            else:
                # forward down
                self._update_parent_producer(lambda p, d: p.subquery(ast, relation))
                return self

        else:
            if self.is_local():
                # build on top: the default behaviour
                Operator.subquery(self, ast, relation)
                return SubQuery.make(self, ast, relation)
            else:
                # Simple behaviour : since all children can be executed in parallel,
                # let's add the child ast as a new children of the current SQ node.
                #MANDO|return SubQuery.add_child(self, ast, relation)
                return self.add_child(self, ast, relation)
