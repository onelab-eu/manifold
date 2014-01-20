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
from manifold.core.code             import GATEWAY
from manifold.core.consumer         import Consumer
from manifold.core.node             import Node 
from manifold.core.packet           import Packet, ErrorPacket
from manifold.core.pit              import Pit
from manifold.core.producer         import Producer
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
from manifold.core.result_value     import ResultValue 
from manifold.core.socket           import Socket 
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.type             import accepts, returns

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(Producer):
    
    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'

    #---------------------------------------------------------------------------  
    # Static methods
    #---------------------------------------------------------------------------  

    @staticmethod
    def register_all():
        """
        Register each available Manifold Gateway.
        """
        current_module = sys.modules[__name__]
        PluginFactory.register(current_module)

    #---------------------------------------------------------------------------  
    # Constructor
    #---------------------------------------------------------------------------  

    def __init__(self, interface, platform_name, platform_config = None, *args, **kwargs):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
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

        Producer.__init__(self, *args, **kwargs)
        self._interface       = interface       # Router
        self._platform_name   = platform_name   # String
        self._platform_config = platform_config # dict
        self._announces       = None            # list(Announces)
        self._capabilities    = Capabilities()  # XXX in the meantime we support all capabilities
        self._pit             = Pit(self)       # Pit

    #---------------------------------------------------------------------------  
    # Accessors
    #---------------------------------------------------------------------------  

    #@returns(Interface)
    def get_interface(self):
        """
        Returns:
            The Interface instance using this Gateway. Most of time
            this is a Router instance.
        """
        return self._interface

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

    @returns(list)
    def get_announces(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        # We do not instanciate make_announces in __init__
        # to allow child classes to tweak their metadata.
        if not self._announces:
            self._announces = self.make_announces()
        return self._announces

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

    @returns(Pit)
    def get_pit(self):
        """
        Returns:
            The PIT of this Gateway.
        """
        return self._pit

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

    @returns(Socket)
    def get_socket(self, query):
        """
        Retrieve the Socket used to return ERROR and RECORD Packet
        related to a given Query. A Socket is automatically created
        if this query is not yet pending.
        Args:
            query: A Query instance.
        Returns:
            The corresponding Socket (if any)
        """
        assert isinstance(query, Query),\
            "Invalid query = %s (%s)" % (query, type(query))

        try:
            socket = self._pit.get_socket(query)
        except KeyError, e:
            Log.warning(traceback.format_exc())
            Log.warning("Can't find query = %s in %s's Pit: %s" % (
                query,
                self.__class__.__name__,
                self._pit
            ))
            raise e
        return socket

    def add_flow(self, query, consumer):
        """
        Add a consumer issuing a given Query on this Gateway.
        Args:
            query: A Query instance correponding to a pending Query.
            consumer: A Consumer instance (a From instance most of time). 
        """
        self._pit.add_flow(query, consumer)

    def del_consumer(self, receiver, cascade = True):
        """
        Unlink a Consumer from this Gateway. 
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to remove 'self'
                to the producers set of 'consumer'.
        """
        self.get_pit().del_receiver(receiver)
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
        query = packet.get_query()
        socket = self.get_socket(query)

        # Clear PIT
        self.get_pit().del_query(query)

        # Unlink this Socket from its Customers 
        socket.close()
        
    #---------------------------------------------------------------------------  
    # Query plan optimization. 
    #---------------------------------------------------------------------------  

    @returns(Node)
    def optimize_selection(self, query, filter):
        """
        Propagate a WHERE clause through a FROM Node.
        Args:
            query: The Query received by this Gateway.
            filter: A Filter instance. 
        Returns:
            The updated root Node of the sub-AST.
        """
        # XXX Simplifications
#FIXME|        for predicate in filter:
#FIXME|            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
#FIXME|                # The result of the request is empty, no need to instanciate any gateway
#FIXME|                # Replace current node by an empty node
#FIXME|                return FromTable(query, [], self.key)
#FIXME|            # XXX Note that such issues could be detected beforehand

        Log.tmp("optimize_selection: query = %s filter = %s" % (query, filter))
        capabilities = self.get_capabilities(query.get_from())

        if capabilities.selection:
            # Push filters into the From node
            query.filter_by(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        else:
            # Create a new Selection node
            selection = Selection(self, filter)

            # XXX fullquery ?
            if capabilities.fullquery:
                # We also push the filter down into the node
                for predicate in filter:
                    query.filters.add(predicate)

            return selection

    @returns(Node)
    def optimize_projection(self, query, fields):
        """
        Propagate a SELECT clause through a FROM Node.
        Args:
            query: The Query received by this Gateway.
            fields: A set of String instances (queried fields).
        Returns:
            The updated root Node of the sub-AST.
        """
        Log.tmp("optimize_projection: query = %s fields = %s" % (query, fields))
        capabilities = self.get_capabilities(query.get_from())
        if capabilities.projection:
            # Push fields into the From node
            self.query.select().select(fields)
            return self
        else:
            provided_fields = self.get_table(query.get_from()).get_field_names()

            # Test whether this From node can return every queried Fields.
            if fields - provided_fields:
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by %s. Available fields are: {%s}" % (
                    ", ".join(list(fields - provided_fields)),
                    query.get_from(),
                    ", ".join(list(provided_fields))
                )) 

            # If this From node returns more Fields than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if provided_fields - fields:
                # XXX fullquery ?
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self

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
        return self.get_interface().execute_local_query(query)

    def record_impl(self, socket, record):
        """
        (Internal usage) Send a Record in a Socket.
        Args:
            socket: The Socket which has to transport the Record.
            record: The Record we want to send. 
        """
        socket.receive(record if isinstance(record, Record) else Record(record))

    def record(self, packet, record):
        """
        Helper used in Gateway when a has to send an ERROR Packet. 
        See also Gateway::records() instead.
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            record: A Record or a dict instance. If this is the only
                Packet that must be returned, turn on the LAST_RECORD
                flag otherwise the Gateway will freeze.
                Example:
                    my_record = Record({"field" : "value"})
                    my_record.set_last(True)
                    self.record(my_record)
        """
        self.check_query_packet(packet)
        socket = self.get_socket(packet.get_query())
        self.record_impl(socket, record)

    def records(self, packet, records):
        """
        Helper used in Gateway when a has to send several RECORDS Packet. 
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            record: A Records or a list of instances that may be
                casted in Record (e.g. Record or dict instances).
        """
        socket = self.get_socket(packet.get_query())

        # Print debugging information
        # TODO refer properly pending Socket of each Gateway because
        # that's why we do not simply run socket.get_producer().format_uptree()
        Log.debug(
            "UP-TREE:\n--------\n%s\n%s" % (
                socket.get_producer().format_node(),
                socket.format_uptree()
            )
        )

        if records:
            # Enable LAST_RECORD flag on the last Record 
            if isinstance(records[-1], dict):
                records[-1] = Record(records[-1], last = True)
            else:
                records[-1].set_last()

            # Send the records
            for record in records:
                self.record_impl(socket, record)
        else:
            self.record_impl(socket, Record(last = True))

    def warning(self, packet, description):
        """
        Helper used in Gateway when a has to send an ERROR Packet
        carrying an Warning. See also Gateway::error() 
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            description: The corresponding warning message (String) or
                Exception.
        """
        self.error(packet, description, False)
        
    def error(self, packet, description, is_fatal = True):
        """
        Helper used in Gateway when a has to send an ERROR Packet
        carrying an Error. See also Gateway::warning() 
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            description: The corresponding error message (String) or
                Exception.
            is_fatal: Set to True if this ERROR Packet must stops
                the Record retrieval. Pass True if the Gateway won't
                send back more Packet for the current Query, False
                otherwise.
        """
        try:
            self.check_query_packet(packet)
            if issubclass(type(description), Exception):
                description = "%s" % description
            assert isinstance(description, StringTypes),\
                "Invalid description = %s (%s)" % (description, type(description))
            assert isinstance(is_fatal, bool),\
                "Invalid is_fatal = %s (%s)" % (is_fatal, type(is_fatal))
        except Exception, e:
            Log.error(e)

        # Could be factorized with Operator::error() by defining Producer::error()
        socket = self.get_socket(packet.get_query())
        error_packet = self.make_error(GATEWAY, description, is_fatal)
        socket.receive(error_packet)

    def receive(self, packet):
        """
        Handle a incoming QUERY Packet (processing).
        Classes inheriting Gateway must not overload this method, they
        must overload Gateway::receive_impl() instead.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_receive(packet)

        try:
            # This method must be overloaded on the Gateway
            # See manifold/gateways/template/__init__.py
            self.receive_impl(packet) 
        except Exception, e:
            #print "RECEIVE EXCEPTION", e
            self.error(packet, e, True)
#        finally:
#            self.close(packet)

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

    @returns(list)
    def make_announces(self):
        """
        Build the list of Announces corresponding to this Gateway.
        This method should be overloaded/overwritten if the Gateway
        announces Tables not referenced in a dedicated .h file/docstring.
        """
        return Announces.from_dot_h(self.get_platform_name(), self.get_gateway_type())

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet. This callback must
        be overloaded in each Gateway. See Gateway::receive().
        Args:
            packet: A QUERY Packet instance.
        """
        raise NotImplementedError, "receive_impl must be overloaded"


