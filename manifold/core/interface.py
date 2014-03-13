#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json, traceback
from types                          import GeneratorType, StringTypes
#from twisted.internet.defer         import Deferred

from manifold.gateways              import Gateway
from manifold.core.annotation       import Annotation
from manifold.core.capabilities     import Capabilities
from manifold.core.code             import BADARGS, ERROR
from manifold.core.packet           import QueryPacket, ErrorPacket, Packet
from manifold.core.query            import Query
from manifold.core.result_value     import ResultValue
from manifold.core.socket           import Socket
from manifold.core.sync_receiver    import SyncReceiver
from manifold.policy                import Policy
from manifold.util.log              import Log
from manifold.util.predicate        import eq, included
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

DEFAULT_GATEWAY_TYPE = "manifold"

class Interface(object):

    #---------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------

    def __init__(self, allowed_capabilities = None):
        """
        Create an Interface instance.
        Args:
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Interface. Pass None if there
                is no restriction.
        """
        assert not allowed_capabilities or isinstance(allowed_capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (allowed_capabilities, type(allowed_capabilities))

        # Register the Gateways
        self.register_gateways()

        # Storage
        self._storage = None

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

        self.boot()

    def __enter__(self):
        """
        Function called back while entering a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().start_reactor()
        return self

    def __exit__(self, type = None, value = None, traceback = None):
        """
        Function called back while leaving a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().stop_reactor()

    #---------------------------------------------------------------------
    # Announces / capabilities
    #---------------------------------------------------------------------

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

    #---------------------------------------------------------------------
    # Storage XXX THIS SHOULD DISAPPEAR OUTSIDE OF THE Interface/Router classes
    #---------------------------------------------------------------------

    def set_storage(self, storage):
        """
        Install a Storage on this Router.
        See also:
            router::load_storage()
        Args:
            storage: A Storage instance.
        """
        #assert isinstance(storage, Storage)
        self._storage = storage

    def load_storage(self, platform_names = None):
        """
        Load from the Storage a set of Platforms.
        Args:
            platform_names: A set/frozenset of String where each String
                is the name of a Platform. If you pass None,
                all the Platform not disabled in the Storage
                are considered.
        """
        assert not platform_names or isinstance(platform_names, (frozenset, set)),\
            "Invalid platform_names = %s (%s)" % (platform_names, type(platform_names))

        # Fetch enabled Platforms from the Storage...
        if not platform_names:
            query = Query.get('platform').filter_by("disabled", eq, False)
            platforms_storage = self.execute_local_query(query)
        else:
            query = Query.get('platform').filter_by("platform", included, platform_names)
            platforms_storage = self.execute_local_query(query)

            # Check whether if all the requested Platforms have been found in the Storage.
            platform_names_storage = set([platform["platform"] for platform in platforms_storage])
            platform_names_missing = platform_names - platform_names_storage 
            if platform_names_missing:
                Log.warning("The following platform names are undefined in the Storage: %s" % platform_names_missing)

        # ... and register them in this Router.
        for platform in platforms_storage:
            self.register_platform(platform)

        # Enabled/disable Platforms related to this Router
        # according to this new set of Platforms.
        self.update_platforms(platforms_storage)

        # Load policies from Storage
        self.policy.load()

    @returns(bool)
    def has_storage(self):
        """
        Returns:
            True iif this Router has a Storage.
        """
        try:
            return self.get_storage() != None
        except:
            pass
        return False

    #@returns(Storage)
    def get_storage(self):
        """
        Returns:
            The Gateway used to query the Manifold Storage.
        """
        if not self._storage:
            raise RuntimeError("Unable to connect to the Manifold Storage")
        return self._storage


    #---------------------------------------------------------------------
    # Platform management.
    #---------------------------------------------------------------------

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
        gateway_type    = platform.get('gateway_type', DEFAULT_GATEWAY_TYPE)
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

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        # The Storage must be explicitely installed if needed.
        # See example in manifold.clients.local
        if self.has_storage():
            Log.tmp("Loading Manifold Storage...")
            self.load_storage()

    def receive(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A QUERY Packet instance.
        """
        assert isinstance(packet, Packet),\
            "Invalid packet %s (%s) (%s) (invalid type)" % (packet, type(packet))

        # Create a Socket holding the connection information and bind it.
        socket = Socket()
        # XXX !!!!!
        packet.get_receiver()._set_child(socket)
        packet.set_receiver(socket)

        self.process_query_packet(packet)

    def process_query_packet(self, packet):
        """
        """
        # Build the AST and retrieve the corresponding root_node Operator instance.
        query      = packet.get_query()
        annotation = packet.get_annotation()
        receiver   = packet.get_receiver()

        try:
            root_node = self._operator_graph.build_query_plan(query, annotation)

            print "QUERY PLAN:"
            print root_node.format_downtree()

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
            print "ECC"
            traceback.print_exc()
            receiver.receive(error_packet)

    def execute_query(self, query, error_message):
        """
        """

        # XXX We should benefit from caching if rules allows for it possible
        # XXX LOCAL


        if error_message:
            Log.warning("error_message not taken into account")

        # Build a query packet
        receiver = SyncReceiver()
        packet = QueryPacket(query, Annotation(), receiver)
        self.process_query_packet(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value.get_all().to_dict_list()

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
        from manifold.util.storage import STORAGE_NAMESPACE
        query.set_namespace(STORAGE_NAMESPACE)
        return self.execute_query(query, error_message)
