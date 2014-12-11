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

from manifold.core.annotation       import Annotation
from manifold.core.announce         import Announces, Announce
from manifold.core.capabilities     import Capabilities
from manifold.core.code             import BADARGS, ERROR
from manifold.core.destination      import Destination
# DEPRECATED BY FIBfrom manifold.core.dbnorm           import to_3nf   # Replaced by FIB
# DEPRECATED BY FIBfrom manifold.core.dbgraph          import DBGraph  # Replaced by FIB
from manifold.core.fib              import FIB
from manifold.core.operator_graph   import OperatorGraph
from manifold.core.packet           import ErrorPacket, Packet, GET, CREATE, UPDATE, DELETE
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.core.table            import Table
from manifold.gateways              import Gateway
from manifold.interfaces            import Interface
from manifold.policy                import Policy
from manifold.util.constants        import DEFAULT_PEER_URL, DEFAULT_PEER_PORT
from manifold.util.log              import Log
from manifold.util.type             import returns, accepts

from manifold.core.local            import LocalGateway, LOCAL_NAMESPACE

DEFAULT_GATEWAY_TYPE = "manifold"

#------------------------------------------------------------------
# Class Router
#------------------------------------------------------------------

class Router(object):

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
        assert not allowed_capabilities or isinstance(allowed_capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (allowed_capabilities, type(allowed_capabilities))

        # Manifold Gateways are already initialized in parent class.
        self._operator_graph = OperatorGraph(router = self)

        # Register the Gateways and interfaces
        Gateway.register_all()
        Interface.register_all()

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities
        if self.allowed_capabilities:
            Log.warning("Router::__init__(): 'allowed_capabilities' parameter not yet supported")

        # self.data is {String : list(Announce)} dictionnary mapping each
        # platform name (= namespace) with its corresponding Announces.
        self.announces = dict()

        # self.policy is a Policy object implementing kind of iptables
        # allowing to filter Packets (Announces and so on).
        self.policy = Policy(self)

        # DBGraphs
        self._dbgraph = None

        # FIB
        self._fib = FIB(self)

        # interface_uuid -> interface
        self._interfaces = dict()

        # We request announces from the local gateway (cf # manifold.core.interface)
        # XXX This should be similar for all gateways
        # XXX add_platform
        self.add_platform(LOCAL_NAMESPACE, LOCAL_NAMESPACE)

# DEPRECATED BY FIB        self._local_gateway = LocalGateway(router = self)
# DEPRECATED BY FIB        self._local_dbgraph = self.get_local_gateway().make_dbgraph()

        # A dictionary mapping the method to the cache for local subqueries,
        # which itself will be a dict matching the parent record uuids to the
        # list of records associated with a subquery
        self._local_cache = dict()

    def terminate(self):
        for interface in self._interfaces.values():
            interface.terminate()

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_fib(self):
        return self._fib

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

# DEPRECATED BY FIB    @returns(DBGraph)
# DEPRECATED BY FIB    def get_dbgraph(self):
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        Returns:
# DEPRECATED BY FIB            The DBGraph related to all the Tables except those
# DEPRECATED BY FIB            provided by the Manifold Storage.
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        return self._dbgraph

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def boot(self):
        """
        Boot the Router (prepare metadata, etc.).
        """
        self.cache = dict()
        super(Router, self).boot()

# DEPRECATED BY FIB    def rebuild_dbgraph(self):
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        Internal usage, should be called when self.announces is altered.
# DEPRECATED BY FIB        Recompute the global DbGraph.
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        self._dbgraph = to_3nf(self.get_announces())
# DEPRECATED BY FIB        self._local_dbgraph = self.get_local_gateway().make_dbgraph()

    def register_local_collection(self, cls):
        self.get_interface('local').register_collection(cls, 'local')

    def register_collection(self, cls, namespace=None):
        self.get_interface('local').register_collection(cls, namespace)

    #---------------------------------------------------------------------------
    # Platform management
    #---------------------------------------------------------------------------

    # XXX TO DEPRECATE
    @returns(bool)
    def add_peer(self, platform_name, hostname, port = DEFAULT_PEER_PORT):
        """
        Connect this router to another Manifold Router and fetch the
        corresponding Announces.
        Args:
            platform_name: A String containing the platform_name (= namespace)
                corresponding to this peer.
            hostname: A String containing the IP address or the hostname
                of the Manifold peer.
            port: An integer value on which the Manifold peer listen for
                Manifold query. It typically runs a manifold-router process
                listening on this port.
        Returns:
            True iif successful.
        """
        assert isinstance(platform_name, StringTypes) and platform_name
        assert isinstance(hostname,      StringTypes) and hostname
        assert isinstance(port,          int)

        print "add peer should use an interface... until this is merged with gateways"

        url = DEFAULT_PEER_URL % locals()
        return self.add_platform(platform_name, "manifold", {"url" : url})

    # XXX TO DEPRECATE
    # This will be the only calls for manipulating router platforms
    @returns(Gateway)
    def add_platform(self, platform_name, gateway_type, platform_config = None):
        """
        Add a platform (register_platform + enable_platform) in this Router
        and fetch the corresponding Announces.
        Args:
            platform_name: A String containing the name of the Platform.
            gateway_type: A String containing the type of Gateway used for this Platform.
            platform_config: A dictionnary { String : instance } containing the
                configuration of this Platform.
        Returns:
            True iif successful.
        """
        if not platform_config:
            platform_config = dict()

        try:
            Log.info("Adding platform [%s] (type: %s, config: %s)" % (platform_name, gateway_type, platform_config))
            if gateway_type == LOCAL_NAMESPACE:
                gateway = LocalGateway(router = self)
            else:
                gateway = self.make_gateway(platform_name, gateway_type, platform_config)

#DEPRECATED|            print "REQUESTING ANNOUNCES FOR PLATFORM", platform_name
#DEPRECATED|            # Retrieving announces from gateway, and populate the FIB
#DEPRECATED|            packet = GET()
#DEPRECATED|            packet.set_source(self.get_fib().get_address())
#DEPRECATED|            packet.set_destination(Destination('object', namespace='local'))
#DEPRECATED|            packet.set_receiver(self.get_fib())
#DEPRECATED|            gateway.send(packet)

#DEPRECATED|            announces = gateway.get_announces()
#DEPRECATED|            for announce in announces:
#DEPRECATED|                self._fib.add(platform_name, announce)

            #self._interfaces[platform_name] = gateway

        except Exception, e:
            Log.warning(traceback.format_exc())
            Log.warning("Error while adding %(platform_name)s[%(platform_config)s] (%(platform_config)s): %(e)s" % locals())
            return None

        return gateway

    def register_interface(self, interface):
        self._interfaces[interface.get_uuid()] = interface

    def unregister_interface(self, interface):
        platform_name = interface.get_uuid()
        del self._interfaces[platform_name]
        self.get_fib().remove_platform(platform_name)

    def add_interface(self, interface_type, *args, **kwargs):
        interface_cls = Interface.factory_get(interface_type)
        if not interface_cls:
            Log.warning("Could not create a %(interface_type)s interface" % locals())
            return None

        router_args = (self,) + args
        interface = interface_cls(*router_args, **kwargs)
        # Note the interface will register itself when initialized properly
        # This is needed to have interfaces dynamically created by # TCPServerSocketInterface
        interface.up()
        return interface

    def is_interface_up(self, interface_name):
        interface = self._interface.get(interface_name)
        if not interface:
            return False
        return self._interface[interface_name].is_up()

    @returns(bool)
    def del_platform(self, platform_name, rebuild = True):
        """
        Remove a platform from this Router. This platform is no more
        registered. The corresponding Announces are also removed.
        Args:
            platform_name: A String containing a platform name.
            rebuild: True if the DbGraph must be rebuild.
        Returns:
            True if it altered this Router.
        """
        ret = False
        try:
            # DEPRECATED
            del self._interfaces[platform_name]
            ret = True
        except KeyError:
            pass

        self.disable_platform(platform_name, rebuild)
        return ret

#    @returns(GeneratorType)
#    def get_platforms(self):
#        """
#        Returns:
#            A Generator allowing to iterate on list of dict where each
#            dict represents a Platform managed by this Router.
#        """
#        for platform_name, gateway in self.gateways.items():
#            yield gateway.get_config()
#
#    @returns(dict)
#    def get_platform(self, platform_name):
#        """
#        Retrieve the dictionnary representing a platform for a given
#        platform name.
#        Args:
#            platform_name: A String containing the name of the platform.
#        Raises:
#            KeyError: if platform_name is unknown.
#        Returns:
#            The corresponding Platform if found, None otherwise.
#        """
#        assert isinstance(platform_name, StringTypes),\
#            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))
#
#        return self.gateways[platform_name].get_config()
#
#    @returns(set)
#    def get_registered_platform_names(self):
#        """
#        Returns:
#            A set of String where each String is a platform name having
#            a Gateway configured in this Router. It is not necessarily
#            enabled.
#        """
#        return set(self.gateways.keys())
#
#    @returns(set)
#    def get_enabled_platform_names(self):
#        """
#        Returns:
#            A set of String where each String is the name of an enabled
#            (and registered) platform.
#        """
#        return set(self.announces.keys())

    @returns(bool)
    def register_platform(self, platform):
        """
        Register a platform in this Router.
        Args:
            platform: A dict describing a Platform.
        Returns:
            True iif successful
        """
        assert isinstance(platform, dict),\
            "Invalid platform = %s (%s)" % (platform, type(platform))

        ret = False
        platform_name   = platform["platform"]
        gateway_type    = platform["gateway_type"]
        platform_config = platform["config"]

        try:
            #Log.info("Registering platform [%s] (type: %s, config: %s)" % (platform_name, gateway_type, platform_config))
            gateway = self.make_gateway(platform_name, gateway_type, platform_config)

            self._interfaces[platform_name] = gateway

            ret = True
        except Exception:
            Log.warning(traceback.format_exc())

        return ret

#DEPRECATED|    def update_platforms(self, new_platforms_enabled):
#DEPRECATED|        """
#DEPRECATED|        Update the Gateways and Announces loaded by this Router according
#DEPRECATED|        to a list of platforms. This function should be called whenever
#DEPRECATED|        a Platform is enabled/disabled without explictely call
#DEPRECATED|        {dis|en}able_platform.
#DEPRECATED|        Args:
#DEPRECATED|            new_platforms_enabled: The list of platforms which must be enabled. All
#DEPRECATED|                the other platforms are automaticaly disabled.
#DEPRECATED|        """
#DEPRECATED|        Log.warning("Ignored update platforms")
#DEPRECATED|        return
#DEPRECATED|        assert set(self.gateways.keys()) >= set(self.announces.keys())
#DEPRECATED|
#DEPRECATED|        old_platform_names_enabled  = self.get_enabled_platform_names()
#DEPRECATED|        new_platform_names_enabled = set([platform["platform"] for platform in new_platforms_enabled])
#DEPRECATED|
#DEPRECATED|        platform_names_del = old_platform_names_enabled - new_platform_names_enabled
#DEPRECATED|        platform_names_add = new_platform_names_enabled - old_platform_names_enabled
#DEPRECATED|
#DEPRECATED|        router_altered = False
#DEPRECATED|
#DEPRECATED|        for platform_name in platform_names_del:
#DEPRECATED|            router_altered |= self.disable_platform(platform_name, False)
#DEPRECATED|
#DEPRECATED|        for platform_name in platform_names_add:
#DEPRECATED|            try:
#DEPRECATED|                router_altered |= self.enable_platform(platform_name, False)
#DEPRECATED|            except RuntimeError, e:
#DEPRECATED|                Log.warning(traceback.format_exc())
#DEPRECATED|                Log.warning("Cannot enable platform '%s': %s" % (platform_name, e))
#DEPRECATED|                pass
#DEPRECATED|
#DEPRECATED|# DEPRECATED BY FIB        if router_altered:
#DEPRECATED|# DEPRECATED BY FIB            self.rebuild_dbgraph()
#DEPRECATED|
#DEPRECATED|    @returns(bool)
#DEPRECATED|    def disable_platform(self, platform_name, rebuild = True):
#DEPRECATED|        """
#DEPRECATED|        Unload a platform (e.g its correponding Gateway and Announces).
#DEPRECATED|        Args:
#DEPRECATED|            platform_name: A String containing a platform supported by this Router.
#DEPRECATED|                Most of time, platform names corresponds to contents in "platform"
#DEPRECATED|                column of "platform" table of the Manifold Storage.
#DEPRECATED|            rebuild: True if the DbGraph must be rebuild.
#DEPRECATED|        Returns:
#DEPRECATED|            True iif it altered the state of this Router.
#DEPRECATED|        """
#DEPRECATED|        if platform_name == LOCAL_NAMESPACE:
#DEPRECATED|            # The LocalGateway is always enabled.
#DEPRECATED|            return False
#DEPRECATED|
#DEPRECATED|        Log.info("Disabling platform '%s'" % platform_name)
#DEPRECATED|        ret = False
#DEPRECATED|
#DEPRECATED|        if platform_name in self.announces.keys():
#DEPRECATED|            del self.announces[platform_name]
#DEPRECATED|# DEPRECATED BY FIB            if rebuild: self.rebuild_dbgraph()
#DEPRECATED|            ret = True
#DEPRECATED|        else:
#DEPRECATED|            Log.warning("Cannot disable %s (not enabled)"  % platform_name)
#DEPRECATED|
#DEPRECATED|        return ret
#DEPRECATED|
#DEPRECATED|    @returns(bool)
#DEPRECATED|    def enable_platform(self, platform_name, rebuild = True):
#DEPRECATED|        """
#DEPRECATED|        Enable a platform (e.g. pull the Announces from the corresponding Gateway).
#DEPRECATED|        This platform must be previously registered (see self.gateways).
#DEPRECATED|        Args:
#DEPRECATED|            platform_name: A String containing a platform supported by this Router.
#DEPRECATED|                Example: See in Manifold Storage table "platform", column "platform".
#DEPRECATED|            rebuild: True if the DbGraph must be rebuild.
#DEPRECATED|        Returns:
#DEPRECATED|            True iif it altered the state of this Router.
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(platform_name, StringTypes),\
#DEPRECATED|            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))
#DEPRECATED|
#DEPRECATED|        Log.info("Enabling platform '%s'" % platform_name)
#DEPRECATED|        ret = False
#DEPRECATED|
#DEPRECATED|        try:
#DEPRECATED|            gateway = self.get_interface(platform_name)
#DEPRECATED|
#DEPRECATED|            # Load Announces related to this Platform
#DEPRECATED|            announces = gateway.get_announces()
#DEPRECATED|            assert isinstance(announces, Announces),\
#DEPRECATED|                "%s::get_announces() should return an Announces: %s (%s)" % (
#DEPRECATED|                    gateway.__class__.__name__,
#DEPRECATED|                    announces,
#DEPRECATED|                    type(announces)
#DEPRECATED|                )
#DEPRECATED|
#DEPRECATED|            # Install the Announces corresponding to this Platform in this Router.
#DEPRECATED|            self.announces[platform_name] = announces
#DEPRECATED|# DEPRECATED BY FIB            if rebuild: self.rebuild_dbgraph()
#DEPRECATED|            ret = True
#DEPRECATED|        except Exception, e:
#DEPRECATED|            Log.warning(traceback.format_exc())
#DEPRECATED|            Log.warning("Error while enabling %(platform_name)s: %(e)s" % locals())
#DEPRECATED|            if platform_name in self.announces.keys(): del self.announces[platform_name]
#DEPRECATED|
#DEPRECATED|        return ret

    #---------------------------------------------------------------------
    # Gateways management (internal usage)
    #---------------------------------------------------------------------

    @returns(Interface)
    def get_interface(self, platform_name):
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

        if platform_name in self._interfaces:
            return self._interfaces[platform_name]

        raise RuntimeError("%s is not yet registered" % platform_name)

    def get_interface_names(self):
        return self._interfaces.keys()

    def get_interfaces(self):
        return self._interfaces.items()

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
        cls_gateway = Gateway.factory_get(gateway_type)
        if not cls_gateway:
            raise RuntimeError, "Gateway not found: %s" % gateway_type

        # Create the Gateway
        gateway = cls_gateway(self, platform_name, platform_config)
        return gateway

    #---------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------

# XXX TO BE IMPLEMENTED FOR FIB
# DEPRECATED BY FIB    def hook_query(self, query):
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        Hook an Query to alter the current state of this Router.
# DEPRECATED BY FIB        Args:
# DEPRECATED BY FIB            query: A Query instance
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        namespace = query.get_namespace()
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # FROM local:platform
# DEPRECATED BY FIB        if namespace == LOCAL_NAMESPACE and query.get_table_name() == "platform":
# DEPRECATED BY FIB            try:
# DEPRECATED BY FIB                from query                      import ACTION_UPDATE, ACTION_DELETE, ACTION_CREATE
# DEPRECATED BY FIB                from operator                   import eq
# DEPRECATED BY FIB                from manifold.util.predicate    import included
# DEPRECATED BY FIB                impacted_platforms = set()
# DEPRECATED BY FIB
# DEPRECATED BY FIB                # This assumes that we handle WHERE platform == "foo"
# DEPRECATED BY FIB                for predicate in query.get_where():
# DEPRECATED BY FIB                    if predicate.get_key() == "platform":
# DEPRECATED BY FIB                        value = predicate.get_value()
# DEPRECATED BY FIB                        if predicate.get_op() == eq:
# DEPRECATED BY FIB                            impacted_platforms.add(value)
# DEPRECATED BY FIB                        elif predicate.get_op() == contains:
# DEPRECATED BY FIB                            impacted_platforms |= set(value)
# DEPRECATED BY FIB
# DEPRECATED BY FIB                if impacted_platforms != set():
# DEPRECATED BY FIB
# DEPRECATED BY FIB                    # UDATE
# DEPRECATED BY FIB                    if query.get_action() == ACTION_UPDATE:
# DEPRECATED BY FIB                        params = query.get_params()
# DEPRECATED BY FIB                        if "disabled" in params.keys():
# DEPRECATED BY FIB                            becomes_disabled = (params["disabled"] == 1)
# DEPRECATED BY FIB                            # TODO This should be merged with update_platforms
# DEPRECATED BY FIB                            router_altered = False
# DEPRECATED BY FIB                            for platform_name in impacted_platforms:
# DEPRECATED BY FIB                                if becomes_disabled:
# DEPRECATED BY FIB                                    router_altered |= self.disable_platform(platform_name, False)
# DEPRECATED BY FIB                                else:
# DEPRECATED BY FIB                                    router_altered |= self.enable_platform(platform_name, False)
# DEPRECATED BY FIB# DEPRECATED BY FIB                            if router_altered:
# DEPRECATED BY FIB# DEPRECATED BY FIB                                self.rebuild_dbgraph()
# DEPRECATED BY FIB
# DEPRECATED BY FIB                    # DELETE
# DEPRECATED BY FIB                    elif query.get_action() == ACTION_DELETE:
# DEPRECATED BY FIB                        for platform_name in impacted_platforms:
# DEPRECATED BY FIB                            self.del_platform(platform_name)
# DEPRECATED BY FIB
# DEPRECATED BY FIB                    # INSERT
# DEPRECATED BY FIB                    elif query.get_action() == ACTION_CREATE:
# DEPRECATED BY FIB                        params = query.get_params()
# DEPRECATED BY FIB                        # NOTE:
# DEPRECATED BY FIB                        # - if disabled = 1: add_platform (== register_platform + enable_platform)
# DEPRECATED BY FIB                        # - else: i          register_platform
# DEPRECATED BY FIB                        self.register_platform(query.get_params())
# DEPRECATED BY FIB                        try:
# DEPRECATED BY FIB                            if query.get_params()["disabled"] == 1:
# DEPRECATED BY FIB                                self.enable_platform(platform_name)
# DEPRECATED BY FIB                        except:
# DEPRECATED BY FIB                            pass
# DEPRECATED BY FIB
# DEPRECATED BY FIB                    # SELECT, ...
# DEPRECATED BY FIB                    else:
# DEPRECATED BY FIB                        pass
# DEPRECATED BY FIB
# DEPRECATED BY FIB                Log.info("Loaded platforms are now: {%s}" % ", ".join(self.get_enabled_platform_names()))
# DEPRECATED BY FIB            except Exception, e:
# DEPRECATED BY FIB                Log.error(e)
# DEPRECATED BY FIB                raise e

#DEPRECATED|            # Let's assume the record is an announce
#DEPRECATED|            if packet.is_empty():
#DEPRECATED|                return
#DEPRECATED|            packet_dict = packet.to_dict()
#DEPRECATED|            origins = packet_dict.get('origins')
#DEPRECATED|            platform_name = origins[0] if origins else 'local'
#DEPRECATED|            namespace = 'local' if platform_name == 'local' else None
#DEPRECATED|            announce = Announce(Table.from_dict(packet_dict, LOCAL_NAMESPACE))
#DEPRECATED|            self.get_fib().add(platform_name, announce, namespace)

    def receive(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A QueryPacket instance.
        """
        destination = packet.get_destination()

        # Packet for the FIB ?
        if destination == self.get_fib().get_address():
            self.get_fib().receive(packet)
            return

        query      = packet.get_query()
        annotation = packet.get_annotation()
        receiver   = packet.get_receiver()

        try:
            # Check namespace
            namespace = destination.get_namespace()
            valid_namespaces = self.get_fib().get_namespaces()
            if namespace and namespace not in valid_namespaces:
                raise RuntimeError("Invalid namespace '%s': valid namespaces are {'%s'}" % (
                    namespace, "', '".join(valid_namespaces)))

            exclude_interfaces = list()
            if packet._ingress:
                ingress = packet._ingress.get_filter().get_eq('uuid')
                exclude_interfaces.append(ingress)

            # Select the DbGraph answering to the incoming Query and compute the QueryPlan
            root_node = self._operator_graph.build_query_plan(destination, annotation, exclude_interfaces = exclude_interfaces)
            
            if not root_node:
                record = Record(last = True)
                record.set_source(packet.get_destination())
                record.set_destination(packet.get_source())
                receiver.receive(record)
                return

            print "QUERY PLAN:"
            print root_node.format_downtree() 

            receiver._set_child(root_node)
        except Exception, e:
            print "E:", e
            Log.tmp(e)
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS,
                message   = "Unable to build a suitable Query Plan (destination = %s): %s" % (destination, e),
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

        orig_receiver = annotation.pop('receiver', None)
        if orig_receiver:
            if orig_receiver.startswith('file://'):
                orig_receiver = orig_receiver[7:]
            if orig_receiver.endswith('.csv'):
                # We need to create a new csv interface
                object_name = destination.get_object_name()
                gw = Gateway.factory_get('csv')
                config = {
                    object_name: {
                        'filename': orig_receiver,
                        #'fields' : [],
                        #'key'
                    }}
                # XXX This does not work for storing local:XXX
                interface = gw(self, orig_receiver, config)
            else:
                error_packet = ErrorPacket(
                    type      = ERROR,
                    code      = BADARGS,
                    message   = "Invalid receiver: %s" % (receiver, ),
                )
                receiver.receive(error_packet)
                return

            Log.warning("This is a bit harsh if we reuse the results")
            record = Record(last = True)
            record.set_source(packet.get_destination())
            record.set_destination(packet.get_source())
            for c in root_node.get_consumers():
                c.receive(record)
            root_node.clear_consumers()
            root_node.add_consumer(interface)

            # This is not taken into account by the gateway receiver function,
            # since it is a customer of the query plan.
            #packet.set_receiver(interface)
        
        try:
            root_node.send(packet)
        except Exception, e:
            print "EEE:", e
            traceback.print_exc()
            error_packet = ErrorPacket(
                type      = ERROR,
                code      = BADARGS,
                message   = "Unable to execute Query Plan: %s" % (e, ),
                #message   = "Unable to execute Query Plan (destination = %s): %s" % (destination, e),
                traceback = traceback.format_exc()
            )
            receiver.receive(error_packet)

    @returns(list)
    def execute_query(self, query, annotation, error_message):
        """
        Forward a Query 
        Args:
            annotation: An Annotation instance.
            query: A Query instance
            error_message: A String instance
        Returns:
            The corresponding list of Record.
        """
        # XXX We should benefit from caching if rules allows for it possible
        # XXX LOCAL

        if error_message:
            Log.warning("error_message not taken into account")

        receiver = SyncReceiver()

        packet = GET()
        packet.set_destination(query.get_destination())
        packet.set_receiver(receiver)

        self.receive(packet) # process_query_packet(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value.get_all().to_dict_list()


    def execute_local_query(self, query, annotation = None, error_message = None):
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
        return self.execute_query(query, annotation, error_message)

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
