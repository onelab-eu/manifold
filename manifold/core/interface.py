#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold Interface can be either a Manifold Router or
# a Manifold Forwarder. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json
from twisted.internet           import defer

from manifold.gateways          import Gateway
from manifold.core.key          import Keys
from manifold.core.query        import Query
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
from manifold.policy            import Policy
from manifold.models.platform   import Platform 
from manifold.util.storage      import DBStorage as Storage
from manifold.util.type         import accepts, returns 
from manifold.util.log          import Log
from manifold.gateways          import register_gateways

class Interface(object):
    """
    A Manifold standard Interface.
    It stores metadata and is able to build a QueryPlan from a Query.
    """
    # Exposes : forward, get_announces, etc.
    # This is in fact a router initialized with a single gateway
    # Better, a router should inherit interface

    LOCAL_NAMESPACE = "local"

    def __init__(self, allowed_capabilities = None):
        """
        Create an Interface instance.
        Args:
            platforms: A list of Platforms.
            allowed_capabilities: A Capabilities instance or None
        """
        # Register the list of Gateways
        Log.info("Registering gateways")
        register_gateways()

        # self.platforms is list(dict) where each dict describes a platform.
        # See platform table in the Storage.
        self.platforms = Storage.execute(Query().get("platform").filter_by("disabled", "=", False), format = "object")

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities

        # self.data is {String : list(Announce)} dictionnary mapping each
        # platform name (= namespace) with its corresponding Announces.
        self.metadata = dict() 

        # self.gateways is a {String : Gateway} which maps a platform name to
        # the appropriate Gateway instance.
        self.gateways = dict()

        self.policy = Policy(self)

        self.boot()

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        assert isinstance(self.platforms, list), "Invalid platforms"

        for platform in self.platforms:
            # Get platform configuration
            platform_config = platform.config
            if platform_config:
                platform_config = json.loads(platform_config)

            platform_name = platform.platform
            args = [None, platform_name, None, platform_config, {}, None]
            gateway = Gateway.get(platform.gateway_type)(*args)
            announces = gateway.get_metadata()
            self.metadata[platform_name] = list() 
            for announce in announces:
                self.metadata[platform_name].append(announce)

        self.policy.load()

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
        for platform in self.platforms:
            if platform.platform == platform_name:
                return platform
        return None 

    #@returns(Gateway)
    def make_gateway(self, platform_name, user):
        """
        Retrieve the Gateway instance corresponding to a platform name.
        Args:
            platform_name: A String containing the name of the platform.
            user: A User instance.
        Raises:
            Exception: in case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        platform = self.get_platform(platform_name)

        if platform_name == "dummy":
            args = [None, platform_name, None, platform.gateway_config, None, user]
        else:
            platform_config = platform.get_config() 
            user_config = platform.get_user_config(user)
            args = [self, platform_name, None, platform_config, user_config, user]

        return Gateway.get(platform.gateway_type)(*args)

    def instanciate_gateways(self, query_plan, user):
        """
        Instanciate Gateway instances involved in the QueryPlan.
        Args:
            query_plan: A QueryPlan instance, deduced from the user's Query.
            user: A User instance.
        """
        # XXX Platforms only serve for metadata
        # in fact we should initialize filters from the instance, then rely on
        # Storage including those filters...
        for from_node in query_plan.froms:
            platform_name = from_node.get_platform()
            gateway = self.make_gateway(platform_name, user)
            if gateway:
                gateway.set_identifier(from_node.get_identifier())
                from_node.set_gateway(gateway)

    # XXX TODO factorize with forward
    @returns(list)
    def get_metadata_objects(self):
        """
        Returns:
            A list of dictionnaries describing each 3nf Tables.
        """
        table_dicts = dict() 

        # XXX not generic
        for table in self.g_3nf.graph.nodes():
            # Ignore non parent tables
            if not self.g_3nf.is_parent(table):
                continue

            table_name = table.get_name()
            fields = set() | table.get_fields()
            for _, child in self.g_3nf.graph.out_edges(table):
                if not child.get_name() == table_name:
                    continue
                fields |= child.get_fields()

            # Build columns from fields
            columns = table_dicts[table_name]["column"] if table_name in table_dicts.keys() else list()
            for field in fields:
                column = field.to_dict()
                assert "name" in column.keys(), "Invalid field dict" # DEBUG
                if column not in columns:
                    columns.append(column)

            keys = tuple(table.get_keys().one().get_field_names())

            table_dicts[table_name] = {
                "table"      : table_name,
                "column"     : columns,
                "key"        : keys,
                "capability" : list(),
            }
        return table_dicts.values()

    @returns(Keys)
    def metadata_get_keys(self, table_name):
        """
        Retrieve the keys related to a given Table.
        Params:
            table_name: A String containing the name of the Table.
        Returns:
            Keys instance related to this 3nf Table.
        """
        return self.g_3nf.find_node(table_name).get_keys()

    def forward(self, query, annotations = None, is_deferred = False, execute = True, user = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            is_deferred: (bool)
            execute: Set to true if the QueryPlan must be executed.
            user: The user issuing the Query.
        Returns:
            A ResultValue in case of success.
            None in case of failure.
        """
        Log.info("Incoming query: %r" % query)

        # Enforcing policy
        annotation = None
        accept = self.policy.filter(query, annotation)
        if not accept:
            return ResultValue.get_error(ResultValue.FORBIDDEN)

        # if Interface is_deferred  
        d = defer.Deferred() if is_deferred else None

        # Implements common functionalities = local queries, etc.
        namespace = None

        # Handling internal queries
        if ":" in query.get_from():
            namespace, table_name = query.get_from().rsplit(":", 2)

        if namespace == self.LOCAL_NAMESPACE:
            if table_name in ['object', 'gateway']:
                if table_name == 'object':
                    output = self.get_metadata_objects()
                elif table_name == "gateway":
                    output = [{'name': name} for name in Gateway.list().keys()]
                qp = QueryPlan()
                qp.ast.from_table(query, output, key=None).selection(query.get_where()).projection(query.get_select())
                return qp.execute(d)
                
            else:
                q = query.copy()
                q.object = table_name
                output = Storage.execute(q, user = user)
                output = ResultValue.get_success(output)
                #Log.tmp("output=",output)
                # if Interface is_deferred
                if not d:
                    return output
                else:
                    d.callback(output)
                    return d
        elif namespace:
            platform_names = self.metadata.keys()
            if namespace not in platform_names:
                raise ValueError("Unsupported namespace '%s' (valid values are: %s and local)" % (namespace, ", ".join(self.metadata.keys())))

            if table_name == "object":
                # Prepare 'output' which will contains announces transposed as a list
                # of dictionnaries.
                output = list()
                announces = self.metadata[namespace]
                for announce in announces:
                    output.append(announce.get_table().to_dict())

                qp = QueryPlan()
                qp.ast.from_table(query, output, key = None).selection(query.get_where()).projection(query.get_select())
                return qp.execute(d)

                #output = ResultValue.get_success(output)
                #if not d:
                #    return output
                #else:
                #    d.callback(output)
                #    return d
                
                # In fact we would need a simple query plan here instead
                # Source = list of dict
                # Result = a list or a deferred
     
        # None is returned to inform child classes they are in charge of the answer
        return None
