from types                         import StringTypes
from manifold.core.query           import Query
from manifold.operators            import Node, LAST_RECORD
from manifold.operators.selection  import Selection   # XXX
from manifold.operators.projection import Projection  # XXX
from manifold.util.type            import returns

DUMPSTR_FROM       = "SELECT %s FROM %s::%s WHERE %s" 

#------------------------------------------------------------------
# FROM node
#------------------------------------------------------------------

class From(Node):
    """
    \brief FROM node
    From Node are responsible to query a gateway (!= FromTable).
    """

    def __init__(self, platform, query, capabilities, key):
    #def __init__(self, table, query):
        """
        \brief Constructor
        \param table A Table instance (the 3nf table)
            \sa manifold.core.table.py
        \param query A Query instance: the query passed to the gateway to fetch records 
        """
        assert isinstance(query, Query), "Invalid type: query = %r (%r)" % (query, type(query))
        # XXX replaced by platform name (string)
        #assert isinstance(table, Table), "Invalid type: table = %r (%r)" % (table, type(table))

        #self.query, self.table = query, table
        self.platform, self.query, self.capabilities, self.key = platform, query, capabilities, key
        self.gateway = None
        super(From, self).__init__()

#    @returns(Query)
#    def get_query(self):
#        """
#        \brief Returns the query representing the data produced by the nodes.
#        \return query representing the data produced by the nodes.
#        """
#        print "From::get_query()"
#        # The query returned by a FROM node is exactly the one that was
#        # requested
#        return self.query

    def add_fields_to_query(self, field_names):
        """
        \brief Add field names (list of String) to the SELECT clause of the embedded query
        """
        for field_name in field_names:
            assert isinstance(field_name, StringTypes), "Invalid field_name = %r in field_names = %r" % (field_name, field_names)
        self.query.fields = frozenset(set(self.query.fields) | set(field_names))

#    #@returns(Table)
#    def get_table(self):
#        """
#        \return The Table instance queried by this FROM node.
#        """
#        return self.table

    @returns(StringTypes)
    def get_platform(self):
        """
        \return The name of the platform queried by this FROM node.
        """
        return self.platform
        #return list(self.get_table().get_platforms())[0]

    #@returns(StringTypes)
    def __repr__(self):
        fields = self.get_query().get_select()
        fields = ', '.join(fields) if fields else '*'
        return DUMPSTR_FROM % (
            fields,
            self.get_platform(),
            self.get_query().get_from(),
            self.get_query().get_where()
        )

    #@returns(From)
    def inject(self, records, key, query):
        """
        \brief Inject record / record keys into the node
        \param records list of dictionaries representing records,
                       or list of record keys
        \return This node
        """
        if not records:
            return
        record = records[0]

        # Are the records a list of full records, or only record keys
        is_record = isinstance(record, dict)

        # If the injection does not provides all needed fields, we need to
        # request them and join
        provided_fields = set(record.keys()) if is_record else set([key])
        needed_fields = self.query.fields
        missing_fields = needed_fields - provided_fields

        old_self_callback = self.get_callback()

        if not missing_fields:
            from_table = FromTable(self.query, records, key)
            from_table.set_callback(old_self_callback)
            return from_table

        # If the inject only provide keys, add a WHERE, otherwise a WHERE+JOIN
        if not is_record or provided_fields < set(records[0].keys()):
            # We will filter the query by inserting a where on 
            list_of_keys = records.keys() if is_record else records
            predicate = Predicate(key, included, list_of_keys)
            where = Selection(self, Filter().filter_by(predicate))
            where.query = self.query.copy().filter_by(predicate)
            where.set_callback(old_self_callback)
            # XXX need reoptimization
            return where
        #else:
        #    print "From::inject() - INJECTING RECORDS"

        missing_fields.add(key) # |= key
        self.query.fields = missing_fields

        #parent_query = self.query.copy()
        #parent_query.fields = provided_fields
        #parent_from = FromTable(parent_query, records, key)

        join = LeftJoin(records, self, Predicate(key, '==', key))
        join.set_callback(old_self_callback)

        return join

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        # @loic Added self.send(LAST_RECORD) if no Gateway is selected, then send no result
        # That might mean that the user has no account for the platform
        if not self.gateway:
            self.send(LAST_RECORD)
            #raise Exception, "Cannot call start on a From class, expecting Gateway"
        else:
            self.gateway.start()

    def set_gateway(self, gateway):
        gateway.set_callback(self.get_callback())
        self.gateway = gateway

    def set_callback(self, callback):
        super(From, self).set_callback(callback)
        if self.gateway:
            self.gateway.set_callback(callback)

    def optimize_selection(self, filter):
        key = self.key.get_field_names()
        print "key", self.query.object, ": ", key
        is_join = self.capabilities.join and filter.get_field_names() < key | set([self.query.object]) == filter.get_field_names()
        print "is_join)", is_join
        if self.capabilities.selection or is_join:
            # Push filters into the From node
            self.query.filter_by(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        else:
            # Create a new Selection node
            old_self_callback = self.get_callback()
            selection = Selection(self, filter)
            #selection.query = self.query.copy().filter_by(filter)
            selection.set_callback(old_self_callback)
            return selection

    def optimize_projection(self, fields):
        if self.capabilities.projection:
            # Push fields into the From node
            self.query.select(fields)
            return self
        else:
            if fields - self.get_query().get_select():
                print "W: Missing fields in From"
            if self.get_query().get_select() - fields:
                # Create a new Projection node
                old_self_callback = self.get_callback()
                projection = Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
                projection.set_callback(old_self_callback)
                return projection
            return self
