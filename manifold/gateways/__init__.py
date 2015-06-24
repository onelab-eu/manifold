#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Register every available Gateways.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC

import json, os, sys, traceback
from types                          import StringTypes

from manifold.core.announce         import Announces
from manifold.core.capabilities     import Capabilities
from manifold.core.code             import CORE, ERROR, GATEWAY
from manifold.core.node             import Node
from manifold.core.packet           import Packet, ErrorPacket
from manifold.core.query            import Query
from manifold.core.record           import Records
from manifold.core.result_value     import ResultValue
from manifold.core.socket           import Socket
from manifold.gateways.object       import ManifoldCollection
from manifold.interfaces            import Interface
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.util.constants        import STATIC_ROUTES_DIR
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.type             import accepts, returns

LOCAL_NAMESPACE = 'local'


# The sets of objects exposed by this gateway
class OLocalLocalObject(ManifoldCollection):
    """
    class object {
        string object_name;      /**< The name of the object/table.        */
        string namespace;
        column columns[];       /**< The corresponding fields/columns.    */
        string capabilities[];  /**< The supported capabilities           */
        string key[];           /**< The keys related to this object      */
        string origins[];       /**< The platform originating this object */

        CAPABILITY(retrieve);
        KEY(object_name);
    };
    """

    def get(self, packet):
        
        objects = list()
        for collection in self.get_gateway().get_collections():
            obj = collection.get_object().get_announce().to_dict()
            objects.append(obj['object'])
        return Records(objects)

class OLocalLocalColumn(ManifoldCollection):
    """
    class column {
        string qualifier;
        string name;
        string type;
        string description;
        bool   is_array;

        LOCAL KEY(name);
    };
    """

#class OLocalObject(ManifoldCollection):
#    """
#    class object {
#        string  table;           /**< The name of the object/table.        */
#        column  columns[];       /**< The corresponding fields/columns.    */
#        string  capabilities[];  /**< The supported capabilities           */
#        string  key[];           /**< The keys related to this object      */
#        string  origins[];       /**< The platform originating this object */
#
#        CAPABILITY(retrieve);
#        KEY(table);
#    };
#    """
#
#    def get(self, query = None):
#        return Records([a.to_dict() for a in self.get_gateway().get_announces()]) # only default namespace for now

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(Interface, Node): # XXX Node needed ?

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'
    registered = False # Added to avoid multiple registrations

    #---------------------------------------------------------------------------
    # Static methods
    #---------------------------------------------------------------------------

    @staticmethod
    def register_all(force = False):
        """
        Register each available Manifold Gateway if not yet done.
        Args:
            force: A boolean set to True enforcing Gateway registration
                even if already done.
        """
        # XXX We should not need such test... it's a coding error and should
        # raise a Fatal exception
        if not Gateway.registered:
            Log.info("Registering gateways")
            current_module = sys.modules[__name__]
            PluginFactory.register(current_module)
            Log.info("Registered gateways are: {%s}" % ", ".join(sorted(Gateway.factory_list().keys())))
            Gateway.registered = True

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router = None, platform_name = None, **platform_config):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform_name: A String storing name of the Platform related to this
                Gateway or None.
            platform_config: A dictionnary containing the configuration related
                to the Platform managed by this Gateway. In practice, it should
                correspond to the following value stored in the Storage verifying

                    SELECT config FROM local:platform WHERE platform == "platform_name"
        """
        assert isinstance(platform_name, StringTypes) or not platform_name, \
            "Invalid platform name: %s (%s)" % (platform_name,   type(platform_name))
        assert isinstance(platform_config, dict) or not platform_config, \
            "Invalid configuration: %s (%s)" % (platform_config, type(platform_config))


        self._platform_name   = platform_name   # String
        Interface.__init__(self, router, platform_name, **platform_config)
        Node.__init__(self)
        self._up = True
        assert router

        self._platform_config = platform_config # dict
        self._announces       = None            # list(Announces)
        self._capabilities    = Capabilities()  # XXX in the meantime we support all capabilities

        # namespace -> (object_name -> obj)
        self._collections_by_namespace = dict()

        self.register_collection(OLocalLocalObject(), 'local')
        self.register_collection(OLocalLocalColumn(), 'local')

    def terminate(self):
        pass

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #@returns(Router)
    def get_router(self):
        """
        Returns:
            The Router using this Gateway.
        """
        return self._router

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The String containing the name of the platform related
            to this Gateway.
        """
        return self._platform_name

    @returns(dict)
    def get_config(self):
        """
        Returns:
            A dictionnary containing the configuration related to
                the Platform managed by this Gateway.
        """
        return self._platform_config

    @returns(StringTypes)
    def get_gateway_type(self):
        """
        Returns:
            The type of this Gateway (String instance)
        """
        gateway_type = self.__class__.__name__
        if gateway_type.endswith("Gateway"):
            gateway_type = gateway_type[:-7]
        return gateway_type.lower()

    get_interface_type = get_gateway_type

    @returns(Announces)
    def get_announces(self):
        """
        Retrieve the Announces corresponding to the Table exposed
        by this Gateway. They are typically used to populate the global
        DBGraph of the Router.
        Returns:
            The corresponding Announces instance.
        """
        return Announces([x.get_announce() for x in self.get_objects()])
#DEPRECATED|        # We do not instanciate make_announces in __init__
#DEPRECATED|        # to allow child classes to tweak their metadata.
#DEPRECATED|        if not self._announces:
#DEPRECATED|            try:
#DEPRECATED|                self._announces = self.make_announces()
#DEPRECATED|            except:
#DEPRECATED|                Log.warning(traceback.format_exc())
#DEPRECATED|                Log.warning("Could not get announces from platform %s. It won't be active" % self.get_platform_name())
#DEPRECATED|                self._announces = Announces()
#DEPRECATED|
#DEPRECATED|        return self._announces

    @returns(Capabilities)
    def get_capabilities(self, table_name):
        """
        Retrieve the Capabilities related to a given table.
        Args:
            table_name: A String containing the name of a Table exposed
                by this Gateway.
        Returns:
            The corresponding Capabilities instance exposed by this Gateway
            if found, Capabilities() otherwise.
        """
        capabilities = self._capabilities.get(table_name, None)
        return capabilities if capabilities else Capabilities()

    #@returns(Table)
    def get_table(self, table_name):
        """
        Retrieve the Table instance corresponding to given table name.
        Args:
            table_name: A String containing the name of a Table exposed
                by this Gateway.
        Return:
            The corresponding Table instance exposed by this Gateway
            if found, None otherwise.
        """
        for announce in self.get_announces():
            table = announce.get_table()
            if table.get_name() == table_name:
                return table
        return None

    #---------------------------------------------------------------------------
    # Parameter checking (internal usage)
    #---------------------------------------------------------------------------

    def check_query_packet(self, packet):
        """
        (Internal usage) Check whether a Packet is a QUERY Packet.
        Args:
            packet: It should be a QUERY Packet instance.
        """
        assert isinstance(packet, Packet), \
            "Invalid packet = %s (%s)" % (packet, type(packet))
        assert packet.get_protocol() == Packet.PROTOCOL_QUERY,\
            "Invalid packet type = %s (%s)" % (packet, type(packet))

    #---------------------------------------------------------------------------
    # Flow management
    #---------------------------------------------------------------------------

#DEPRECATED|    @returns(Socket)
#DEPRECATED|    def get_socket(self, query):
#DEPRECATED|        """
#DEPRECATED|        Retrieve the Socket used to return ERROR and RECORD Packet
#DEPRECATED|        related to a given Query. A Socket is automatically created
#DEPRECATED|        if this query is not yet pending.
#DEPRECATED|        Args:
#DEPRECATED|            query: A Query instance.
#DEPRECATED|        Returns:
#DEPRECATED|            The corresponding Socket (if any)
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(query, Query),\
#DEPRECATED|            "Invalid query = %s (%s)" % (query, type(query))
#DEPRECATED|
#DEPRECATED|        try:
#DEPRECATED|            socket = self._pit.get_socket(query)
#DEPRECATED|        except KeyError, e:
#DEPRECATED|            Log.warning(traceback.format_exc())
#DEPRECATED|            Log.warning("Can't find query = %s in %s's Pit: %s" % (
#DEPRECATED|                query,
#DEPRECATED|                self.__class__.__name__,
#DEPRECATED|                self._pit
#DEPRECATED|            ))
#DEPRECATED|            import pdb; pdb.set_trace()
#DEPRECATED|            raise e
#DEPRECATED|        return socket

    def del_consumer(self, receiver, cascade = True):
        """
        Unlink a Consumer from this Gateway.
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to remove 'self'
                to the producers set of 'consumer'.
        """
#DEPRECATED|        self.get_pit().del_receiver(receiver)
        if cascade:
            receiver.del_producer(self, cascade = False)

    def release(self):
        pass

    def close(self, packet):
        """
        Close the Socket related to a given Query and update
        the PIT consequently.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_query_packet(packet)
        query = Query.from_packet(packet)
        socket = self.get_socket(query)

#DEPRECATED|        # Clear PIT
#DEPRECATED|        self.get_pit().del_query(query)

        # Unlink this Socket from its Customers
        socket.close()

#    #---------------------------------------------------------------------------
#    # Query plan optimization.
#    #---------------------------------------------------------------------------
#
#    @returns(Node)
#    def optimize_selection(self, query, filter):
#        """
#        Propagate a WHERE clause through a FROM Node.
#        Args:
#            query: The Query received by this Gateway.
#            filter: A Filter instance.
#        Returns:
#            The updated root Node of the sub-AST.
#        """
#        # XXX Simplifications
##FIXME|        for predicate in filter:
##FIXME|            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
##FIXME|                # The result of the request is empty, no need to instanciate any gateway
##FIXME|                # Replace current node by an empty node
##FIXME|                return FromTable(query, [], self.key)
##FIXME|            # XXX Note that such issues could be detected beforehand
#
#        Log.tmp("optimize_selection: query = %s filter = %s" % (query, filter))
#        table_name = query.get_table_name()
#        capabilities = self.get_capabilities(table_name)
#
#        if capabilities.selection:
#            # Push filters into the From node
#            query.filter_by(filter)
#            #old for predicate in filter:
#            #old    self.query.filters.add(predicate)
#            return self
#        else:
#            # Create a new Selection node
#            selection = Selection(self, filter)
#
#            # XXX fullquery ?
#            if capabilities.fullquery:
#                # We also push the filter down into the node
#                for predicate in filter:
#                    query.filters.add(predicate)
#
#            return selection
#
#    @returns(Node)
#    def optimize_projection(self, query, fields):
#        """
#        Propagate a SELECT clause through a FROM Node.
#        Args:
#            query: The Query received by this Gateway.
#            fields: A set of String instances (queried fields).
#        Returns:
#            The updated root Node of the sub-AST.
#        """
#        Log.tmp("optimize_projection: query = %s fields = %s" % (query, fields))
#        table_name = query.get_table_name()
#        capabilities = self.get_capabilities(table_name)
#
#        if capabilities.projection:
#            # Push fields into the From node
#            self.query.select().select(fields)
#            return self
#        else:
#            provided_fields = self.get_table(table_name).get_field_names()
#
#            # Test whether this From node can return every queried Fields.
#            if fields - provided_fields:
#                Log.warning("Gateway::optimize_projection: some requested fields (%s) are not provided by %s. Available fields are: {%s}" % (
#                    ", ".join(list(fields - provided_fields)),
#                    query.get_from(),
#                    ", ".join(list(provided_fields))
#                ))
#
#            # If this From node returns more Fields than those explicitely queried
#            # (because the projection capability is not enabled), create an additional
#            # Projection Node above this From Node in order to guarantee that
#            # we only return queried fields
#            if provided_fields - fields:
#                # XXX fullquery ?
#                return Projection(self, fields)
#                #projection.query = self.query.copy().filter_by(filter) # XXX
#            return self

    #---------------------------------------------------------------------------
    # Helpers for child classes
    #---------------------------------------------------------------------------

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    @returns(dict)
    def get_variables(user, account_config):
        """
        ???
        Args:
            user: A dictionnary corresponding to the User.
            account_config: A dictionnary corresponding to the user's Account config.
        Returns:
            The corresponding dictionnary.
        """
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = dict()

        # Authenticated user
        variables["user_email"] = user["email"]
        if user:
            for k, v in user["config"].items():
                if isinstance(v, StringTypes) and not "credential" in v:
                    variables[k] = v
        # Account information of the authenticated user
        for k, v in account_config.items():
            if isinstance(v, StringTypes) and not "credential" in v:
                variables[k] = v
        return variables

    @returns(list)
    def query_storage(self, query):
        """
        Run a Query on the Manifold's Storage.
        Args:
            query: A Query instance.
        Returns:
            A list of dictionnaries corresponding to each fetched Records.
        """
        return self.get_router().execute_local_query(query)

    # Use of the two following functions ? -- jordan

    def handle_failure(self, failure, query_packet):
        e = failure.trap(Exception)
        self.error(query_packet, str(e))

    @returns(bool)
    def handle_query_object(self, packet):
        ret = False
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            query = Query.from_packet(packet)
            table_name = query.get_table_name()
            if table_name == "object":
                action = query.get_action()
                if action != "get":
                    raise ValueError("Invalid action (%s) on %s:%s Table" % (action, self.get_platform_name(), table_name))
                self.records([announce.to_dict() for announce in self.get_announces()], packet)
                ret = True
        return ret

    #---------------------------------------------------------------------------
    # Methods that could/must be overloaded/overwritten in the child classes
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return repr(self)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Gateway.
        """
        return "Gateway<%s>[%s]" % (
            self.get_platform_name(),
            self.get_gateway_type()
        )

    @returns(Announces)
    def make_announces(self):
        """
        Returns:
            The Announces instance corresponding to this Gateway.
            This method should be overloaded/overwritten if the Gateway
            announces Tables not referenced in a dedicated .h file/docstring.
        """
        return Announces.parse_static_routes(STATIC_ROUTES_DIR, self.get_platform_name(), self.get_gateway_type())

    # TODO Rename Producer::make_error() into Producer::error()
    # and retrieve the appropriate consumers and send to them
    # the ErrorPacket that has been crafted
    @returns(ErrorPacket)
    def make_error(self, origin, description, is_fatal):
        """
        Craft an ErrorPacket carrying an error message.
        Args:
            description: The corresponding error message (String) or
                Exception.
            origin: An integer indicated who raised this error.
                Valid values are {CORE, GATEWAY}
            description: A String containing the error message.
            is_fatal: Set to True if this ErrorPacket
                must make crash the pending Query.
        Returns:
            The corresponding ErrorPacket.
        """
        assert isinstance(description, StringTypes),\
            "Invalid description = %s (%s)" % (description, type(description))
        # Note: 'origin' is ignored for the moment
        # Note: 'type'   is ignored for the moment
        assert origin in [CORE, GATEWAY],\
            "Invalid origin = %s (%s)" % (origin, type(origin))
        assert isinstance(is_fatal, bool),\
            "Invalid is_fatal = %s (%s)" % (is_fatal, type(is_fatal))

        if is_fatal:
            Log.error(description)
        else:
            Log.warning(description)
        error_packet = ErrorPacket(ERROR, origin, description, traceback.format_exc())
        error_packet.set_last(is_fatal)
        return error_packet

    # Flow management is disabled in Gateways, so special treatment here doing
    # the minimum...
    def _manage_incoming_flow(self, packet):
        return True
    def _manage_outgoing_flow(self, packet):
        receiver = packet.get_receiver()
        if receiver:
            return receiver
        else:
            return self._router

    def send_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        destination = packet.get_destination()
        
        namespace   = destination.get_namespace()
        object_name = destination.get_object_name()

        try:
            collection = self.get_collection(object_name, namespace)
        except ValueError:
            raise RuntimeError("Invalid object '%s::%s'" % (namespace, object_name))

        # This is because we assure the gateway could modify the packet, which
        # is further used in self.records
        packet_clone = packet.clone()

        if packet.get_protocol() == Packet.PROTOCOL_CREATE:
            collection.create(packet_clone)
            self.last(packet)
            return
        elif packet.get_protocol() == Packet.PROTOCOL_GET:
            # Do not wait results when the receiver has been changed
            records = collection.get(packet_clone)
        else:
            raise NotImplemented

        # Asynchronous gateways return None,
        # Others should return an empty list
        if records is None:
            return

        if records:
            self.records(records, packet)
        else:
            self.last(packet)

    # We overload the interface receive function
    def receive(self, packet, **kwargs):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
            kwargs are ignored, present for compatibility with operators.
        """
        print "GATEWAY RECV", packet
        source = packet.get_source()
        
        namespace   = source.get_namespace()
        object_name = source.get_object_name()

        try:
            collection = self.get_collection(object_name, namespace)
        except ValueError:
            raise RuntimeError("Invalid object '%s::%s'" % (namespace, object_name))

        # This is because we assure the gateway could modify the packet, which
        # is further used in self.records
        packet_clone = packet.clone()

        assert packet.get_protocol() == Packet.PROTOCOL_CREATE
        collection.create(packet_clone)

        # For now, we return since we are processing packet by packet
        return

    def get_collection(self, object_name, namespace = None):
        return self._collections_by_namespace[namespace][object_name]

    def get_collections(self, namespace = None):
        if not namespace in self._collections_by_namespace:
            return list()
        return self._collections_by_namespace[namespace].values()

    def register_collection(self, collection, namespace = None):
        # Register it in the FIB: we ignore the announces in the local namespace
        # unless the platform_name is local
        collection.set_gateway(self)

        cls = collection.get_object()
        platform_name = self.get_platform_name()
        object_name = cls.get_object_name()

        if platform_name == 'local' or namespace != 'local' or object_name not in ['object', 'column']:
            self.get_router().get_fib().add(platform_name, cls.get_announce(), namespace)

        # Store the object locally
        if namespace not in self._collections_by_namespace:
            self._collections_by_namespace[namespace] = dict()
        self._collections_by_namespace[namespace][cls.get_object_name()] = collection

#DEPRECATED|        # Fetch Announces produced by the Storage
#DEPRECATED|        gateway_storage = self._storage.get_gateway()
#DEPRECATED|        if gateway_storage:
#DEPRECATED|            #local_announces = local_announces | gateway_storage.get_announces()
#DEPRECATED|            local_announces |= gateway_storage.get_announces()
#DEPRECATED|
#DEPRECATED|        # Fetch Announces produced by each enabled platform.
#DEPRECATED|        router = self.get_router()
#DEPRECATED|        if router:
#DEPRECATED|            for platform_name in router.get_enabled_platform_names():
#DEPRECATED|                gateway = router.get_gateway(platform_name)
#DEPRECATED|                # foo:object is renamed local:object since we cannot compute query plan
#DEPRECATED|                # over the local DBGraph if its table are not attached to platform "local"
#DEPRECATED|                local_announces |= make_local_announces(LOCAL_NAMESPACE)
#DEPRECATED|        else:
#DEPRECATED|            Log.warning("The router of this %s is unset. Some Announces cannot be fetched" % self)
#DEPRECATED|
#DEPRECATED|        return local_announces

# DEPRECATED BY FIB    @returns(DBGraph)
# DEPRECATED BY FIB    def make_dbgraph(self):
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        Make the DBGraph.
# DEPRECATED BY FIB        Returns:
# DEPRECATED BY FIB            The DBGraph related to the Manifold Storage.
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        # We do not need normalization here, can directly query the Gateway
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 1) Fetch the Storage's announces and get the corresponding Tables.
# DEPRECATED BY FIB        local_announces = self.get_announces()
# DEPRECATED BY FIB        local_tables = frozenset([announce.get_table() for announce in local_announces])
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 2) Build the corresponding map of Capabilities
# DEPRECATED BY FIB        map_method_capabilities = dict()
# DEPRECATED BY FIB        for announce in local_announces:
# DEPRECATED BY FIB            table = announce.get_table()
# DEPRECATED BY FIB            platform_names = table.get_platforms()
# DEPRECATED BY FIB            assert len(platform_names) == 1, "An announce should be always related to a single origin"
# DEPRECATED BY FIB            table_name = table.get_name()
# DEPRECATED BY FIB            platform_name = iter(platform_names).next()
# DEPRECATED BY FIB            method = Method(platform_name, table_name)
# DEPRECATED BY FIB            capabilities = table.get_capabilities()
# DEPRECATED BY FIB            map_method_capabilities[method] = capabilities
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 3) Build the corresponding DBGraph
# DEPRECATED BY FIB        return DBGraph(local_tables, map_method_capabilities)

