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
from manifold.core.packet           import ErrorPacket, Packet
from manifold.core.query            import Query
#from manifold.core.result_value     import ResultValue
from manifold.core.socket           import Socket
from manifold.policy                import Policy
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

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
        ReactorThread._drop()
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
    # Storage
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
            platforms_storage = self.execute_local_query(
                Query()\
                    .get("platform")\
                    .filter_by("disabled", "==", False)
            )
        else:
            platforms_storage = self.execute_local_query(
                Query()\
                    .get("platform")\
                    .filter_by("platform", "INCLUDED", platform_names)
            )

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

    @returns(list)
    def execute_local_query(self, query, annotation = None, error_message = None):
        """
        Run a Query on the Manifold Storage embeded by this Router.
        Args:
            query: A Query instance.
            annotation: An Annotation instance passed to the Storage's Gateway.
            error_message: A String containing the message to print in case of failure.
        Returns:
            A list of dict corresponding to the Records resulting from
            the query.
        """
        from manifold.util.storage import STORAGE_NAMESPACE

        if query.get_from().startswith("%s:" % STORAGE_NAMESPACE):
            query.clear_namespace()
        return self.get_storage().execute(query, annotation, error_message)

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
        gateway = self.make_gateway(platform_name)

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
            self.make_gateway(platform_name)

        return self.gateways[platform_name]

    @returns(Gateway)
    def make_gateway(self, platform_name):
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

        # Fetch information needed to create the Gateway corresponding to this platform.
        platform = self.get_platform(platform_name)

        # gateway_type
        if platform["gateway_type"]:
            gateway_type = platform["gateway_type"]
        else:
            DEFAULT_GATEWAY_TYPE = "manifold"
            Log.warning("No gateway_type set for platform '%s'. Defaulting to '%s'." % (
                platform["platform"],
                DEFAULT_GATEWAY_TYPE
            ))
            gateway_type = DEFAULT_GATEWAY_TYPE

        # platform_config
        platform_config = json.loads(platform["config"]) if platform["config"] else dict()

        # Get the Gateway class
        cls_gateway = Gateway.get(gateway_type)
        if not cls_gateway:
            raise RuntimeError, "Gateway not found: %s" % platform["gateway_type"]

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
        socket = Socket(consumer = packet.get_receiver())
        packet.set_receiver(socket)

        # Build the AST and retrieve the corresponding root_node Operator instance.
        query = packet.get_query()
        annotation = packet.get_annotation()

        try:
            root_node = self._operator_graph.build_query_plan(query, annotation)
            root_node.add_consumer(socket)
            socket.receive(packet)
        except Exception, e:
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS,
                message   = "Unable to build a suitable Query Plan (query = %s): %s" % (query, e),
                traceback = traceback.format_exc()
            )
            socket.receive(error_packet)

#UNUSED|    @returns(dict)
#UNUSED|    def get_account_config(self, platform_name, user):
#UNUSED|        """
#UNUSED|        Retrieve the Account of a given User on a given Platform.
#UNUSED|        Args:
#UNUSED|            platform_name: A String containing the name of the Platform.
#UNUSED|            user: A dict describing the User who executes the QueryPlan (None if anonymous).
#UNUSED|        Returns:
#UNUSED|            The corresponding dictionnary, None if no account found for
#UNUSED|            this User and this Platform.
#UNUSED|        """
#UNUSED|        assert isinstance(platform_name, StringTypes),\
#UNUSED|            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))
#UNUSED|
#UNUSED|        if platform_name == STORAGE_NAMESPACE:
#UNUSED|            return dict()
#UNUSED|
#UNUSED|        annotation = Annotation({"user" : self.get_user_storage()})
#UNUSED|
#UNUSED|        # Retrieve the Platform having the name "platform_name" in the Storage
#UNUSED|        try:
#UNUSED|            platforms = self.execute_local_query(
#UNUSED|                Query().get("platform").filter_by("platform", "=", platform_name),
#UNUSED|                annotation,
#UNUSED|            )
#UNUSED|            platform_id = platforms[0]["platform_id"]
#UNUSED|        except IndexError:
#UNUSED|            Log.error("interface::get_account_config(): platform %s not found" % platform_name)
#UNUSED|            return None
#UNUSED|
#UNUSED|        # Retrieve the first Account having the name "platform_name" in the Storage
#UNUSED|        try:
#UNUSED|            accounts = self.execute_local_query(
#UNUSED|                Query().get("account").filter_by("platform_id", "=", platform_id),
#UNUSED|                annotation
#UNUSED|            )
#UNUSED|        except IndexError:
#UNUSED|            Log.error("interface::get_account_config(): no account found for platform %s" % platform_name)
#UNUSED|            return None
#UNUSED|
#UNUSED|        #accounts2 = self.execute_local_query(
#UNUSED|        #    Query()\
#UNUSED|        #        .get("account")\
#UNUSED|        #        .select("config")\
#UNUSED|        #        .filter_by("platform", "=", platform_name)#\
#UNUSED|        #        .filter_by("email",    "=", user["email"])\
#UNUSED|        #)
#UNUSED|
#UNUSED|        # Convert the json string "config" into a python dictionnary
#UNUSED|        num_accounts = len(accounts)
#UNUSED|        if num_accounts > 0:
#UNUSED|            if num_accounts > 1:
#UNUSED|                Log.warning("Several accounts found for [%s]@%s: %s" % (user["email"], platform_name, accounts))
#UNUSED|            account = accounts[0]
#UNUSED|            account["config"] = json.loads(account["config"])
#UNUSED|        else:
#UNUSED|            account = None
#UNUSED|
#UNUSED|        return account
#UNUSED|
#UNUSED|    @returns(list)
#UNUSED|    def get_metadata_objects(self):
#UNUSED|        """
#UNUSED|        Returns:
#UNUSED|            A list of dictionnaries describing each 3nf Tables.
#UNUSED|        """
#UNUSED|        output = list()
#UNUSED|        # TODO try to factor using table::to_dict()
#UNUSED|        for table in self.g_3nf.graph.nodes():
#UNUSED|            # Ignore non parent tables
#UNUSED|            if not self.g_3nf.is_parent(table):
#UNUSED|                continue
#UNUSED|
#UNUSED|            table_name = table.get_name()
#UNUSED|
#UNUSED|            # We may have several table having the same name but related
#UNUSED|            # to two different platforms set.
#UNUSED|            fields = set() | table.get_fields()
#UNUSED|            for _, child in self.g_3nf.graph.out_edges(table):
#UNUSED|                if not child.get_name() == table_name:
#UNUSED|                    continue
#UNUSED|                fields |= child.get_fields()
#UNUSED|
#UNUSED|            # Build columns from fields
#UNUSED|            columns = list()
#UNUSED|            for field in fields:
#UNUSED|                columns.append(field.to_dict())
#UNUSED|
#UNUSED|            keys = tuple(table.get_keys().one().get_field_names())
#UNUSED|
#UNUSED|            # Add table metadata
#UNUSED|            output.append({
#UNUSED|                "table"      : table_name,
#UNUSED|                "column"     : columns,
#UNUSED|                "key"        : keys,
#UNUSED|                "capability" : list(),
#UNUSED|            })
#UNUSED|        return output
#UNUSED|
#DEPRECATED|    def send_result_value(self, query, result_value, annotation, is_deferred):
#DEPRECATED|        # if Interface is_deferred
#DEPRECATED|        d = defer.Deferred() if is_deferred else None
#DEPRECATED|
#DEPRECATED|        if not d:
#DEPRECATED|            return result_value
#DEPRECATED|        else:
#DEPRECATED|            d.callback(result_value)
#DEPRECATED|            return d
#DEPRECATED|
#DEPRECATED|    def process_qp_results(self, query, records, annotation, query_plan):
#DEPRECATED|        # Enforcing policy
#DEPRECATED|        (decision, data) = self.policy.filter(query, records, annotation)
#DEPRECATED|        if decision != Policy.ACCEPT:
#DEPRECATED|            raise Exception, "Unknown decision from policy engine"
#DEPRECATED|
#DEPRECATED|        description = query_plan.get_result_value_array()
#DEPRECATED|        return ResultValue.get_result_value(records, description)
#DEPRECATED|
#DEPRECATED|    def execute_query_plan(self, query, annotation, query_plan, is_deferred = False):
#DEPRECATED|        records = query_plan.execute(is_deferred)
#DEPRECATED|        if is_deferred:
#DEPRECATED|            # results is a deferred
#DEPRECATED|            records.addCallback(lambda records: self.process_qp_results(query, records, annotation, query_plan))
#DEPRECATED|            return records # will be a result_value after the callback
#DEPRECATED|        else:
#DEPRECATED|            return self.process_qp_results(query, records, annotation, query_plan)
#DEPRECATED|
