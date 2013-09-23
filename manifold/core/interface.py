#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold Interface can be either a Manifold Router or
# a Manifold Forwarder. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json, traceback
from types                          import GeneratorType
from twisted.internet.defer         import Deferred

from manifold.gateways.gateway      import Gateway
from manifold.core.query            import Query
from manifold.core.query_plan       import QueryPlan
from manifold.core.result_value     import ResultValue
from manifold.models.platform       import Platform
from manifold.models.user           import User
from manifold.util.storage          import DBStorage as Storage
from manifold.util.type             import accepts, returns 
from manifold.util.log              import Log

class Interface(object):
    """
    A Manifold standard Interface.
    It stores metadata and is able to build a QueryPlan from a Query.

    Exposes : forward, get_announces, etc.
    """

    LOCAL_NAMESPACE = "local"

    #---------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------

    def __init__(self, user = None, allowed_capabilities = None):
        """
        Create an Interface instance.
        Args:
            user: A User instance (used to access to the Manifold Storage) or None
                if the Storage can be accessed anonymously.
            allowed_capabilities: A Capabilities instance or None
        """
        # self.platforms is list(dict) where each dict describes a platform.
        # See platform table in the Storage.
        self.storage = Storage(self) 
        self.platforms = list()
        self.storage_user = user

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities

        # self.data is {String : list(Announce)} dictionnary mapping each
        # platform name (= namespace) with its corresponding Announces.
        self.announces = dict() 

        # self.gateways is a {String : Gateway} which maps a platform name to
        # the appropriate Gateway instance.
        self.gateways = dict()
        self.boot()

    #---------------------------------------------------------------------
    # Accessors 
    #---------------------------------------------------------------------

    @returns(dict)
    def get_announces(self):
        """
        Returns:
            A dict {String => list(Announce)} where each key is a Platform
            name and is mapped with the corresponding list of Announces.
        """
        return self.announces

    @returns(GeneratorType)
    def get_platforms(self):
        """
        Returns;
            A Generator allowing to iterate on Platform managed by this Interface.
        """
        for platform in self.platforms:
            yield platform

    @returns(Platform)
    def get_platform(self, platform_name):
        """
        Retrieve the dictionnary representing a platform for a given
        platform name
        Args:
            platform_name: A String containing the name of the platform.
        Returns:
            The corresponding Platform if found, None otherwise.
        """
        for platform in self.get_platforms():
            if platform.platform == platform_name:
                return platform
        return None 

    @returns(Gateway)
    def get_gateway(self, platform_name):
        """
        Prepare the Gateway instance corresponding to a platform name.
        Args:
            platform_name: A String containing the name of the platform.
        Raises:
            ValueError: in case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        if platform_name not in self.gateways.keys():
            # This platform is not referenced in the router, try to create the
            # appropriate Gateway.
            try:
                self.make_gateway(platform_name)
            except Exception, e:
                raise ValueError("Cannot find/create Gateway related to platform %s (%s)" % (platform, e))
        return self.gateways[platform_name]

    #---------------------------------------------------------------------
    # Methods 
    #---------------------------------------------------------------------

    @returns(Gateway)
    def make_gateway(self, platform_name):
        """
        Prepare the Gateway instance corresponding to a Platform name.
        Args:
            platform_name: A String containing the name of the Platform.
        Raises:
            Exception: In case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        platform = self.get_platform(platform_name)
        platform_config = platform.get_config() 
        args = [self, platform_name, platform_config]
        return Gateway.get(platform.gateway_type)(*args)

    def make_gateways(self):
        """
        (Re)build Announces related to each enabled platforms
        Delete Announces related to disabled platforms
        """
        platforms_loaded  = set([platform for platform in self.get_platforms()])
        self.platforms    = self.storage.execute(Query().get("platform").filter_by("disabled", "=", False), self.storage_user, "object")
        platforms_enabled = set(self.platforms)
        platforms_del     = platforms_loaded - platforms_enabled 
        platforms_add     = platforms_enabled - platforms_loaded

        for platform in platforms_del:
            # Unreference this platform which not more used
            Log.info("Disabling platform '%r'" % platform) 
            platform_name = platform.platform
            try:
                del self.gateways[platform_name] 
            except:
                Log.error("Cannot remove %s from %s" % (platform_name, self.gateways))

        for platform in platforms_add: 
            # Create Gateway corresponding to the current Platform
            Log.info("Enabling platform '%r'" % platform) 
            platform_name = platform.platform
            gateway = self.make_gateway(platform_name)
            self.gateways[platform_name] = gateway 

            # Load Announces related to this Platform
            announces = gateway.get_metadata()
            assert isinstance(announces, list), "%s::get_metadata() should return a list : %s (%s)" % (
                platform.gateway.__class__.__name__,
                announces,
                type(announces)
            )
            self.announces[platform_name] = announces 

        self.platforms = list(platforms_enabled)

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        self.make_gateways()

    def init_from_nodes(self, query_plan, user):
        """
        Initialize the From Nodes involved in a QueryPlan by:
            - setting the User of each From Node.
            - setting the Gateways used by each From Node.
        Args:
            query_plan: A QueryPlan instance, deduced from the user's Query.
            user: The User who executes the QueryPlan (None if anonymous).
        """
        # XXX Platforms only serve for metadata
        # in fact we should initialize filters from the instance, then rely on
        # Storage including those filters...
        for from_node in query_plan.froms:
            platform_name = from_node.get_platform_name()
            gateway = self.get_gateway(platform_name)
            if gateway:
                from_node.set_gateway(gateway)
                from_node.set_user(user)
            else:
                raise Exception("Cannot instanciate all required Gateways")

    @returns(list)
    def get_metadata_objects(self):
        """
        Returns:
            A list of dictionnaries describing each 3nf Tables.
        """
        output = list() 
        # TODO try to factor using table::to_dict()
        for table in self.g_3nf.graph.nodes():
            # Ignore non parent tables
            if not self.g_3nf.is_parent(table):
                continue

            table_name = table.get_name()

            # We may have several table having the same name but related
            # to two different platforms set.
            fields = set() | table.get_fields()
            for _, child in self.g_3nf.graph.out_edges(table):
                if not child.get_name() == table_name:
                    continue
                fields |= child.get_fields()

            # Build columns from fields
            columns = list() 
            for field in fields:
                columns.append(field.to_dict())

            keys = tuple(table.get_keys().one().get_field_names())

            # Add table metadata
            output.append({
                "table"      : table_name,
                "column"     : columns,
                "key"        : keys,
                "capability" : [],
            })
        return output

    def check_forward(self, query, is_deferred = False, execute = True, user = None, receiver = None):
        """
        Checks whether parameters passed to Interface::forward() are well-formed.
        """
        assert isinstance(query, Query),                  "Invalid Query: %s (%s)"             % (query,       type(query))
        assert isinstance(is_deferred, bool),             "Invalid is_deferred value: %s (%s)" % (execute,     type(execute))
        assert isinstance(execute, bool),                 "Invalid execute value: %s (%s)"     % (is_deferred, type(is_deferred))
        assert not user or isinstance(user, User),        "Invalid User: %s (%s)"              % (user,        type(user))
        assert not receiver or receiver.set_result_value, "Invalid receiver: %s (%s)"          % (receiver,    type(receiver))

    @staticmethod
    def success(receiver, query, result_value = None):
        """
        Shorthand method when Interface::forward is successful.
        Args:
            See Interface::forward.
        """
        assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
        if receiver:
            receiver.set_result_value(
                ResultValue.get_success(result_value)
            )

    @staticmethod
    def error(receiver, query, description = ""):
        """
        Shorthand method when Interface::error is successful.
        Args:
            See Interface::forward.
        """
        assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
        message = "Error in query %s: %s" % (query, description)
        Log.error(description)
        if receiver:
            import traceback
            receiver.set_result_value(
                ResultValue.get_error(
                    message,
                    traceback.format_exc()
                )
            )

    #@returns(Deferred)
    def forward(self, query, is_deferred = False, execute = True, user = None, receiver = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Basically we only handle local queries here. The other queries are in charge
        of the forward() method of the class inheriting Interface.
        Args:
            query: The user's Query.
            is_deferred: A bool set to True if the Query is async.
            execute: A boolean set to True if the QueryPlan must be executed.
            user: The user issuing the Query.
            receiver: An instance supporting the method set_result_value or None.
                receiver.set_result_value() will be called once the Query has terminated.
        Returns:
            A Deferred instance if the Query is async,
            None otherwise (see QueryPlan::execute())
        """
        receiver.set_result_value(None)
        self.check_forward(query, is_deferred, execute, user, receiver)

        # if Interface is_deferred  
        d = Deferred() if is_deferred else None

        # Implements common functionalities = local queries, etc.
        namespace = None

        # Handling internal queries
        if ":" in query.get_from():
            namespace, table_name = query.get_from().rsplit(":", 2)

        if namespace == self.LOCAL_NAMESPACE:
            if table_name == "object":
                list_objects = self.get_metadata_objects()
                qp = QueryPlan()
                qp.ast.from_table(query, list_objects, key = None).selection(query.get_where()).projection(query.get_select())
                Interface.success(receiver, query)
                return qp.execute(d, receiver)
            else:
                query_storage = query.copy()
                query_storage.object = table_name
                output = self.storage.execute(query_storage, user = user)
                result_value = ResultValue.get_success(output)

                if query_storage.get_from() == "platform" and query_storage.get_action() != "get":
                    self.make_gateways()

                if not d:
                    # async
                    Interface.success(receiver, query, result_value)
                    return result_value
                else:
                    # sync
                    d.callback(result_value)
                    Interface.success(receiver, query, result_value)
                    return d
        elif namespace:
            platform_names = [platform.platform for platform in self.get_platforms()]
            if namespace not in platform_names:
                Interface.error(
                    receiver,
                    query,
                    "Unsupported namespace '%s': valid namespaces are platform names ('%s') and 'local'." % (
                        namespace,
                        "', '".join(platform_names)
                    )
                )
                return None

            if table_name == "object":
                # Prepare 'output' which will contains announces transposed as a list
                # of dictionnaries.
                output = list()
                announces = self.announces[namespace]
                for announce in announces:
                    output.append(announce.get_table().to_dict())

                qp = QueryPlan()
                qp.ast.from_table(query, output, key = None).selection(query.get_where()).projection(query.get_select())
                Interface.success(query, receiver)
                return qp.execute(d, receiver)

                #output = ResultValue.get_success(output)
                #if not d:
                #    return output
                #else:
                #    d.callback(output)
                #    return d
                
                # In fact we would need a simple query plan here instead
                # Source = list of dict
                # Result = a list or a deferred
     
        return None
