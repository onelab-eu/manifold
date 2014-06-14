from types                         import StringTypes
from manifold.core.query           import Query
from manifold.operators            import Node
from manifold.operators.selection  import Selection   # XXX
from manifold.operators.projection import Projection  # XXX
from manifold.operators.from_table import FromTable
from manifold.util.type            import returns
from manifold.util.log             import Log

#DUMPSTR_FROM       = "SELECT %s FROM %s::%s WHERE %s" 

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

        super(From, self).__init__()

        #self.query, self.table = query, table
        self.platform, self.query, self.capabilities, self.key = platform, query, capabilities, key
        self.gateway = None

    def add_fields_to_query(self, field_names):
        """
        \brief Add field names (list of String) to the SELECT clause of the embedded query
        """
        for field_name in field_names:
            assert isinstance(field_name, StringTypes), "Invalid field_name = %r in field_names = %r" % (field_name, field_names)
        self.query.fields = frozenset(set(self.query.fields) | set(field_names))

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
        return self.get_query().to_sql(platform=self.get_platform())
        #DUMPSTR_FROM % (
        #    fields,
        #    self.get_platform(),
        #    self.get_query().get_from(),
        #    self.get_query().get_where()
        #)

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
        # @loic Added self.send(LastRecord()) if no Gateway is selected, then send no result
        # That might mean that the user has no account for the platform
        if not self.gateway:
            self.send(LastRecord())
            #raise Exception, "Cannot call start on a From class, expecting Gateway"
        else:
            # Initialize the query embeded by the Gateway using the one deduced from the QueryPlan
            self.gateway.set_query(self.get_query())
            self.gateway.start()

    def set_gateway(self, gateway):
        gateway.set_callback(self.get_callback())
        self.gateway = gateway

    def set_callback(self, callback):
        super(From, self).set_callback(callback)
        if self.gateway:
            self.gateway.set_callback(callback)

    def optimize_selection(self, filter):
        # XXX Simplifications
        for predicate in filter:
            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
                # The result of the request is empty, no need to instanciate any gateway
                # Replace current node by an empty node
                old_self_callback = self.get_callback()
                from_table = FromTable(self.query, [], self.key)
                from_table.set_callback(old_self_callback)
                return from_table
            # XXX Note that such issues could be detected beforehand

        if self.capabilities.selection:
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

            if self.capabilities.fullquery:
                # We also push the filter down into the node
                for p in filter:
                    self.query.filters.add(p)

            return selection

    def optimize_projection(self, fields):
        """
        Propagate a SELECT clause through a FROM Node.
        Args:
            fields: A set of String instances (queried fields).
        """
        if self.capabilities.projection or self.capabilities.fullquery:
            self.query.select().select(fields)

        if self.capabilities.projection:
            # Push fields into the From node
            return self
        else:
            # Provided fields is set to None if it corresponds to SELECT *
            provided_fields = self.get_query().get_select()

            # Test whether this From node can return every queried Fields.
            if provided_fields and fields - provided_fields:
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
                    ', '.join(list(fields - provided_fields)),
                    self.get_query().get_from(),
                    ', '.join(list(provided_fields))
                )) 

            # If this From node returns more Fields than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if not provided_fields or provided_fields - fields:
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self
