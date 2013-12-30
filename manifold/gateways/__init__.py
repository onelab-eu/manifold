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
from manifold.core.consumer         import Consumer
from manifold.core.node             import Node 
from manifold.core.packet           import Packet, ErrorPacket
from manifold.core.pit              import Pit
from manifold.core.producer         import Producer
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
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
    # Internal methods
    #---------------------------------------------------------------------------  

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return "%s [%s]" % (self.get_platform_name(), self.get_gateway_type())

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Gateway.
        """
        return "FROM <Gateway %s [%s]>" % (self.get_platform_name(), self.get_gateway_type())

    #---------------------------------------------------------------------------  
    # Methods
    #---------------------------------------------------------------------------  

    @returns(list)
    def make_announces(self):
        """
        Build the list of Announces corresponding to this Gateway.
        This method may be overloaded/overwritten if the Gateway announces Tables
        not referenced in a dedicated .h file.
        """
        return Announces.from_dot_h(self.get_platform_name(), self.get_gateway_type())

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

#DEPRECATED|    # TODO clean this method and plug it in Router::forward()
#DEPRECATED|    @staticmethod
#DEPRECATED|    def start(user, account_config, query):
#DEPRECATED|        """
#DEPRECATED|        ???
#DEPRECATED|        Args:
#DEPRECATED|            user: A User instance.
#DEPRECATED|            account_config: A dictionnary.
#DEPRECATED|        Returns:
#DEPRECATED|            The corresponding dictionnary.
#DEPRECATED|        """
#DEPRECATED|        Log.tmp("I'm maybe obsolete")
#DEPRECATED|        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
#DEPRECATED|        try:
#DEPRECATED|            # Replaces variables in the Query (predicate in filters and parameters)
#DEPRECATED|            filter = query.get_where()
#DEPRECATED|            params = query.get_params()
#DEPRECATED|            variables = Gateway.get_variables(user, account_config)
#DEPRECATED|
#DEPRECATED|            for predicate in filter:
#DEPRECATED|                value = predicate.get_value()
#DEPRECATED|
#DEPRECATED|                if isinstance(value, (tuple, list)):
#DEPRECATED|                    Log.warning("Ignoring tuple/list value %s (not yet implemented)" % (value,))
#DEPRECATED|                    continue
#DEPRECATED|
#DEPRECATED|                if value and isinstance(value, StringTypes) and value[0] == '$':
#DEPRECATED|                    var = value[1:]
#DEPRECATED|                    if var in variables:
#DEPRECATED|                        predicate.set_value(variables[var])
#DEPRECATED|
#DEPRECATED|            for key, value in params.items():
#DEPRECATED|                # XXX variable support not implemented for lists and tuples
#DEPRECATED|                if isinstance(value, (tuple, list)):
#DEPRECATED|                    continue
#DEPRECATED|
#DEPRECATED|                if value and isinstance(value, StringTypes) and value[0] == '$':
#DEPRECATED|                    var = value[1:]
#DEPRECATED|                    if var in variables and isinstance(variables[var], StringTypes):
#DEPRECATED|                        params[k] = variables[var]
#DEPRECATED|        except Exception, e:
#DEPRECATED|            import traceback
#DEPRECATED|            Log.warning("Exception in start", e)
#DEPRECATED|            traceback.print_exc()

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

    def check_send(self, packet):
        """
        send() method must be used only for ERROR Packets. RECORD Packets
        must be sent using send_record() or send_records().
        Args:
            packet: A Packet instance.
        """
#jo#        super(Gateway, self).check_send(packet)
#jo#        assert packet.get_protocol() ==  Packet.PROTOCOL_ERROR,\
#jo#            "Invalid packet type (%s)" % packet
        # A packet is a packet and should be sent !
        pass

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
            Log.warning("Can't find query = %s in %s's Pit: %s" % (query, self.__class__.__name__, self._pit))
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

    def record(self, query, record):
        """
        Helper used in Gateway when a has to send an ERROR Packet. 
        Args:
            socket: The Socket used to transport the Packet.
                It is usually retrieved using get_socket() method.
            record: A Record or a dict instance.
        """

        socket = self.get_socket(query)
        socket.receive(record if isinstance(record, Record) else Record(record))

    def records(self, query, records):
        """
        Helper used in Gateway when a has to send an ERROR Packet. 
        Args:
            socket: The Socket used to transport the Packet.
                It is usually retrieved using get_socket() method.
            record: A Records or a list of instances that may be
                casted in Record (e.g. Record or dict instances).
        """

        if records:
            records[-1].set_last()
            for record in records:
                self.record(query, record)
        else:
            self.record(query, Record(last = True))

    def error(self, query, type, code, description):
        """
        Helper used in Gateway when a has to send an ERROR Packet. 
        Args:
            socket: The Socket used to transport the Packet.
                It is usually retrieved using get_socket() method.
        """
        socket = self.get_socket(query)
        socket.receive(ErrorPacket(2, 0, "%s" % description, traceback.format_exc()))

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
        
    def close(self, query):
        """
        Close the Socket related to a given Query and update
        the PIT consequently.
        Args:
            query: A Query instance 
        """
        socket = self.get_socket(query)

        # Clear PIT
        self.get_pit().del_query(query)

        # Unlink this Socket from its Customers 
        socket.close()
        
    @returns(Node)
    def optimize_selection(self, query, filter):
        """
        Propagate a WHERE clause through a FROM Node.
        Args:
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

