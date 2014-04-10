#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold router handles Query, compute the corresponding QueryPlan,
# and deduce which Queries must be sent the appropriate Gateways.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json, traceback
from types                          import GeneratorType, StringTypes

from manifold.core.capabilities     import Capabilities
from manifold.core.code             import BADARGS, ERROR
from manifold.core.dbnorm           import to_3nf
from manifold.core.dbgraph          import DBGraph
from manifold.core.helpers          import execute_query as execute_query_helper
from manifold.core.interface        import Interface
from manifold.core.operator_graph   import OperatorGraph
from manifold.core.packet           import ErrorPacket, Packet
from manifold.gateways              import Gateway
from manifold.policy                import Policy
from manifold.util.log              import Log
from manifold.util.type             import returns, accepts

from manifold.core.local            import LocalGateway, LOCAL_NAMESPACE

DEFAULT_GATEWAY_TYPE = "manifold"

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
# Router class is an Interface:
# builds the query plan, and execute query plan using deferred if required
#------------------------------------------------------------------

# TODO remove Interface inheritance
class Router(Interface):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, allowed_capabilities = None):
        """
        Constructor.
        Args:
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Router. Pass None if there
                is no restriction.
        """
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so
        # that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self)

        assert not allowed_capabilities or isinstance(allowed_capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (allowed_capabilities, type(allowed_capabilities))

        # Manifold Gateways are already initialized in parent class.
        self._operator_graph = OperatorGraph(router = self)


        # Register the Gateways
        self.register_gateways()

        # self.platforms is {String : dict} mapping each platform_name with
        # a dictionnary describing the corresponding Platform.
        # See "platform" table in the Manifold Storage.
        self.platforms = dict()

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities
        if self.allowed_capabilities:
            Log.warning("Interface::__init__(): 'allowed_capabilities' parameter not yet supported")

        # self.data is {String : list(Announce)} dictionnary mapping each
        # platform name (= namespace) with its corresponding Announces.
        self.announces = dict()

        # self.gateways is a {String : Gateway} which maps a platform name to
        # the appropriate Gateway instance.
        self.gateways = dict()

        # self.policy is a Policy object implementing kind of iptables
        # allowing to filter Packets (Announces and so on).
        self.policy = Policy(self)

        # DBGraphs
        self._dbgraph = None

        self._local_gateway = LocalGateway(interface=self)
        self._local_dbgraph = self._local_gateway.get_dbgraph()

        # A dictionary mapping the method to the cache for local subqueries,
        # which itself will be a dict matching the parent record uuids to the
        # list of records associated with a subquery
        self._local_cache = dict()

    def terminate(self):
        for gateway in self.gateways.values():
            gateway.terminate()

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(dict)
    def get_announces(self):
        """
        Returns:
            A dict {String => list(Announce)} where each key is a Platform
            name and is mapped with the corresponding list of Announces.
        """
        return self.announces

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Returns:
            The Capabilities supported by this Router,
            None if every Capabilities are supported.
        """
        return self.allowed_capabilities

    @returns(LocalGateway)
    def get_local_gateway(self):
        """
        Returns:
            The LocalGateway attached to this Router/
        """
        return self._local_gateway

    @returns(DBGraph)
    def get_dbgraph(self):
        """
        Returns:
            The DBGraph related to all the Tables except those
            provided by the Manifold Storage.
        """
        return self._dbgraph

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        self.cache = dict()
        super(Router, self).boot()

    def update_platforms(self, platforms_enabled):
        """
        Update the Gateways and Announces loaded by this Router according
        to a list of platforms. This function should be called whenever
        a Platform is enabled/disabled without explictely call
        {dis|en}able_platform.
        Args:
            platforms_enabled: The list of platforms which must be enabled. All
                the other platforms are automaticaly disabled.
        """
        assert set(self.gateways.keys()) == set(self.announces.keys())

        platform_names_loaded  = set([platform["platform"] for platform in self.gateways.keys()])
        platform_names_enabled = set([platform["platform"] for platform in platforms_enabled])

        platform_names_del     = platform_names_loaded  - platform_names_enabled
        platform_names_add     = platform_names_enabled - platform_names_loaded

        for platform_name in platform_names_del:
            self.disable_platform(platform_name)

        for platform_name in platform_names_add:
            try:
                self.enable_platform(platform_name)
            except Exception, e:
                Log.warning(traceback.format_exc())
                Log.warning("Cannot enable platform '%s': %s" % (platform_name, e))
                pass

        self._dbgraph = to_3nf(self.get_announces())

    #---------------------------------------------------------------------------
    # Platform management
    #---------------------------------------------------------------------------

    # This will be the only calls for manipulating router platforms
    def add_platform(self, platform_name, gateway_type, platform_config = {}):
        Log.info("Enabling platform [%s] (type: %s, config: %s)" % (platform_name, gateway_type, platform_config))
        gateway = self.make_gateway(platform_name, gateway_type, platform_config)
        announces = gateway.get_announces()

        # DUP ??
        self.platforms[platform_name] = None # XXX
        self.gateways[platform_name]  = gateway
        self.announces[platform_name] = announces

        # Update
        self._dbgraph = to_3nf(self.get_announces())

    def del_platform(self, platform_name):
        pass

    # OLD ####

    @returns(GeneratorType)
    def get_platforms(self):
        """
        Returns:
            A Generator allowing to iterate on list of dict where each
            dict represents a Platform managed by this Interface.
        """
        for platform in self.platforms.values():
            yield platform

    def get_platform_names(self):
        return self.platforms.keys()

    @returns(dict)
    def get_platform(self, platform_name):
        """
        Retrieve the dictionnary representing a platform for a given
        platform name.
        Args:
            platform_name: A String containing the name of the platform.
        Raises:
            KeyError: if platform_name is unknown.
        Returns:
            The corresponding Platform if found, None otherwise.
        """
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        return self.platforms[platform_name]

    def disable_platform(self, platform_name):
        """
        Unload a platform (e.g its correponding Gateway and Announces).
        Args:
            platform_name: A String containing a platform supported by this Router.
                Most of time, platform names corresponds to contents in "platform"
                column of "platform" table of the Manifold Storage.
        """
        Log.info("Disabling platform '%s'" % platform_name)

        # Unload the corresponding Gateway
        try:
            del self.gateways[platform_name]
        except:
            Log.error("Cannot remove %s from %s" % (platform_name, self.gateways))

        # Unload the corresponding Announces
        try:
            del self.announces[platform_name]
        except:
            Log.error("Cannot remove %s from %s" % (platform_name, self.announces))

    def register_platform(self, platform):
        """
        Register a platform in this Router.
        Args:
            platform: A dict describing a Platform.
        """
        assert isinstance(platform, dict),\
            "Invalid platform = %s (%s)" % (platform, type(platform))

        platform_name = platform["platform"]
        self.platforms[platform_name] = platform

    def enable_platform(self, platform_name):
        """
        Enable a platform (e.g its correponding Gateway and Announces).
        This platform must be previously registered. See also:
            Interface::register_platform
            Interface::register_platforms_from_storage
        Raises:
            RuntimeError: in case of failure while instantiating the corresponding
                Gateway.
        Args:
            platform_name: A String containing a platform supported by this Router.
                Example: See in Manifold Storage table "platform", column "platform".
        """
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        Log.info("Enabling platform '%s'" % platform_name)

        # Check whether the platform is registered
        if platform_name not in self.platforms.keys():
            raise RuntimeError("Platform %s not yet registered" % platform_name)

        # Create Gateway corresponding to the current Platform
        platform = self.get_platform(platform_name)
        gateway_type = platform.get("gateway_type", DEFAULT_GATEWAY_TYPE)
        if not gateway_type:
            gateway_type = DEFAULT_GATEWAY_TYPE
        platform_config = json.loads(platform["config"]) if platform["config"] else dict()

        gateway = self.make_gateway(platform_name, gateway_type, platform_config)

        # Load Announces related to this Platform
        announces = gateway.get_announces()
        assert isinstance(announces, list),\
            "%s::get_announces() should return a list: %s (%s)" % (
                gateway.__class__.__name__,
                announces,
                type(announces)
            )

        # Reference the Gateway and its Announce related to this Platform in this Router.
        self.gateways[platform_name]  = gateway
        self.announces[platform_name] = announces

    #---------------------------------------------------------------------
    # Gateways management (internal usage)
    #---------------------------------------------------------------------

    def register_gateways(self, force = False):
        """
        Register all Gateways supported by this Router.
        This function should be called if a Gateway is added/removed
        in manifold/gateways/ while this Router is running.
        Args:
            force: A boolean set to True enforcing Gateway registration
                even if already done.
        """
        Gateway.register_all(force)

    @returns(Gateway)
    def get_gateway(self, platform_name):
        """
        Retrieve the Gateway instance corresponding to a platform.
        Args:
            platform_name: A String containing the name of the platform.
        Raises:
            ValueError: if platform_name is invalid.
            RuntimeError: in case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        if platform_name.lower() != platform_name:
            raise ValueError("Invalid platform_name = %s, it must be lower case" % platform_name)

        if platform_name == LOCAL_NAMESPACE:
            return self.get_local_gateway()

        if platform_name not in self.gateways.keys():
            # This Platform is not referenced in the Router, try to create the
            # appropriate Gateway.
            platform = self.get_platform(platform_name)
            gateway_type    = platform.get('gateway_type', DEFAULT_GATEWAY_TYPE)
            if not gateway_type:
                gateway_type = DEFAULT_GATEWAY_TYPE
            platform_config = json.loads(platform["config"]) if platform["config"] else dict()

            self.make_gateway(platform_name)

        return self.gateways[platform_name]

    @returns(Gateway)
    def make_gateway(self, platform_name, gateway_type, platform_config):
        """
        Prepare the Gateway instance corresponding to a Platform name.
        Args:
            platform_name: A String containing the name of the Platform.
        Raises:
            RuntimeError: In case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        # Get the Gateway class
        cls_gateway = Gateway.get(gateway_type)
        if not cls_gateway:
            raise RuntimeError, "Gateway not found: %s" % gateway_type

        # Create the Gateway
        args = [self, platform_name, platform_config]
        gateway = cls_gateway(*args)
        return gateway

    #---------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------

#DEPRECATED|    def boot(self):
#DEPRECATED|        """
#DEPRECATED|        Boot the Interface (prepare metadata, etc.).
#DEPRECATED|        """
#DEPRECATED|        # The Storage must be explicitely installed if needed.
#DEPRECATED|        # See example in manifold.clients.local
#DEPRECATED|        if self.has_storage():
#DEPRECATED|            Log.tmp("Loading Manifold Storage...")
#DEPRECATED|            self.load_storage()

    def receive(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A QUERY Packet instance.
        """
        assert isinstance(packet, Packet),\
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        # Create a Socket holding the connection information and bind it.
        # XXX This might not be useful since it only add a level of indirection
        # Let's try removing it
#DEPRECATED|        socket = Socket()
#DEPRECATED|        packet.get_receiver()._set_child(socket)
#DEPRECATED|        packet.set_receiver(socket)

#DEPRECATED|        self.process_query_packet(packet)
#DEPRECATED|
#DEPRECATED|    def process_query_packet(self, packet):
#DEPRECATED|        """
#DEPRECATED|        """
        # Build the AST and retrieve the corresponding root_node Operator instance.
        query      = packet.get_query()
        annotation = packet.get_annotation()
        receiver   = packet.get_receiver()

        try:

            namespace = query.get_namespace()
            dbgraph = self._local_dbgraph if namespace == LOCAL_NAMESPACE else self._dbgraph

            root_node = self._operator_graph.build_query_plan(query, annotation, dbgraph)

            #print "QUERY PLAN:"
            #print root_node.format_downtree()

            receiver._set_child(root_node)
        except Exception, e:
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS,
                message   = "Unable to build a suitable Query Plan (query = %s): %s" % (query, e),
                traceback = traceback.format_exc()
            )
            receiver.receive(error_packet)
            return

        # Forwarding requests:
        # This might raise issues:
        # - during the forwarding of the query (for all gateways)
        # - during the forwarding of results (for synchronous gateways).
        # For async gateways, errors will be handled in errbacks.
        # XXX We should mutualize this error handling core and the errbacks
        try:
            root_node.receive(packet)
        except Exception, e:
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS,
                message   = "Unable to execute Query Plan: %s" % (e, ),
                #message   = "Unable to execute Query Plan (query = %s): %s" % (query, e),
                traceback = traceback.format_exc()
            )
            traceback.print_exc()
            receiver.receive(error_packet)

    execute_query = execute_query_helper

    def execute_local_query(self, query, error_message = None):
        """
        Run a Query on the Manifold Storage embeded by this Router.
        Args:
            query: A Query instance.
            error_message: A String containing the message to print in case of failure.
        Returns:
            A list of dict corresponding to the Records resulting from
            the query.
        """
        query.set_namespace(LOCAL_NAMESPACE)
        return self.execute_query(query, error_message)

    # XXX Unless we don't need local cache anymore (+ __init__)

    def add_to_local_cache(self, object, parent_uuid, subrecords):
        """
        Args:
            parent_uuid (uuid) : the UUID of the parent record
            subrecords (Records) : the list of sub-records corresponding to parent_uuid
        """
        if not object in self._local_cache:
            self._local_cache[object] = dict()
        self._local_cache[object][parent_uuid] = subrecords

    def get_from_local_cache(self, object, parent_uuids):
        map_parent_uuid = self._local_cache.get(object)
        if not map_parent_uuid:
            return Records()

        if not isinstance(parent_uuids, list):
            parent_uuids = [parent_uuids]

        # Note: the list of parent uuids is necessary since the local cache is
        # shared by several concurrent queries.
        records = Records()
        for parent_uuid in parent_uuids:
            subrecords = map_parent_uuid.get(parent_uuid)
            if subrecords:
                records.extend(subrecords)

        # Those returned records have a parent_uuid field that will allow them
        # to be matched with the parent record
        return records
