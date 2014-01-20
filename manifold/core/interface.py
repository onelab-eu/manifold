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
from twisted.internet.defer         import Deferred

from manifold.gateways              import Gateway
from manifold.core.annotation       import Annotation 
from manifold.core.capabilities     import Capabilities
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.core.result_value     import ResultValue
from manifold.policy                import Policy
from manifold.util.log              import Log
from manifold.util.storage          import make_storage, storage_execute, STORAGE_NAMESPACE
from manifold.util.type             import accepts, returns 

class Interface(object):

    #---------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------

    def __init__(self, user_storage = None, allowed_capabilities = None):
        """
        Create an Interface instance.
        Args:
            user_storage: A String containing the email of the user accessing
                the Storage, None if the Storage can be accessed anonymously.
            allowed_capabilities: A Capabilities instance which defines which
                operation can be performed by this Interface. Pass None if there
                is no restriction.
        """
        assert not user_storage or isinstance(user_storage, StringTypes),\
            "Invalid user = %s (%s)" % (user_storage, type(user_storage))
        assert not allowed_capabilities or isinstance(allowed_capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (allowed_capabilities, type(allowed_capabilities))

        # Register the list of Gateways
        Log.info("Registering gateways")
        Gateway.register_all()
        Log.info("Registered gateways are: {%s}" % ", ".join(sorted(Gateway.list().keys())))

        # Prepare Manifold Storage
        self.user_storage = user_storage
        self._storage = make_storage(None)

        # self.platforms is list(dict) where each dict describes a platform.
        # See platform table in the Storage.
        self.platforms = list()

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities
        if self.allowed_capabilities:
            Log.warning("allowed_capabilities parameter not yet supported")

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
        self.policy.load()

    #---------------------------------------------------------------------
    # Accessors 
    #---------------------------------------------------------------------

    @returns(StringTypes)
    def get_user_storage(self):
        """
        Returns:
            The user name describing how to access the Manifold Storage.
        """
        return self.user_storage

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

    @returns(GeneratorType)
    def get_platforms(self):
        """
        Returns:
            A Generator allowing to iterate on list of dict where each
            dict represents a Platform managed by this Interface.
        """
        for platform in self.platforms:
            yield platform

    @returns(dict)
    def get_platform(self, platform_name):
        """
        Retrieve the dictionnary representing a platform for a given
        platform name.
        Args:
            platform_name: A String containing the name of the platform.
        Returns:
            The corresponding Platform if found, None otherwise.
        """
        for platform in self.get_platforms():
            if platform["platform"] == platform_name:
                return platform
        return None 

    @returns(list)
    def execute_local_query(self, query, annotation = None, error_message = None):
        """
        Execute a Query related to the Manifold Storage.
        Args:
            query: A Query. query.get_from() must start with the
                STORAGE_NAMESPACE namespace (see util/storage.py).
            annotation: An Annotation instance related to query or None.
            error_message: A String containing the error_message that must
                be written in case of failure or None.
        Raises:
            Exception: if the Query does not succeed.
        Returns:
            A list of Records.            
        """
        return storage_execute(self.get_storage(), query, annotation, error_message)

    @returns(Gateway)
    def get_gateway(self, gateway_type):
        """
        Prepare the Gateway instance corresponding to a platform name.
        Args:
            gateway_type: A String containing the type of the Gateway.
                It should be a lower case String bases on classes provided
                in manifold/gateways/ (for instance, if FooGateway
                is provided, "foo" is a valid gateway_type). 
        Raises:
            ValueError: in case of failure.
        Returns:
            The corresponding Gateway if found, None otherwise.
        """
        if gateway_type.lower() != gateway_type:
            raise ValueError("Invalid gateway_type = %s, it must be lower case" % gateway_type)

        if gateway_type not in self.gateways.keys():
            # This platform is not referenced in the router, try to create the
            # appropriate Gateway.
            try:
                self.make_gateway(gateway_type)
            except Exception, e:
                Log.error(traceback.format_exc())
                raise ValueError("Cannot find/create Gateway related to platform %s (%s)" % (gateway_type, e))

        try:
            return self.gateways[gateway_type]
        except KeyError:
            Log.error("Unable to retrieve Gateway %s" % gateway_type)
            return None

    @returns(list)
    def get_gateways(self):
        """
        Returns:
            The list of Gateway currently loaded.
        """
        return self.gateways.values()

    @returns(Gateway)
    def get_storage(self):
        """
        Returns:
            The Gateway used to query the Manifold Storage.
        """
        return self._storage

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
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        platform = self.get_platform(platform_name)
        if not platform:
            Log.error("Cannot make Gateway %s" % platform_name)
            return None

        platform_config = json.loads(platform["config"]) if platform["config"] else dict() 
        args = [self, platform_name, platform_config]

        # Gateway is a plugin_factory
        if platform["gateway_type"]:
            gateway_type = platform["gateway_type"]
        else:
            Log.warning("No gateway_type set for platform '%s'. Defaulting to MANIFOLD." % platform["platform"])
            gateway_type = "manifold"

        gateway = Gateway.get(gateway_type)
        if not gateway:
            raise Exception, "Gateway not found: %s" % platform["gateway_type"]
        return gateway(*args)

    def make_gateways(self):
        """
        (Re)build Announces related to each enabled platforms
        Delete Announces related to disabled platforms
        """
        platform_names_loaded = set([platform["platform"] for platform in self.get_platforms()])

        query      = Query().get("platform").filter_by("disabled", "=", False)
        annotation = Annotation({"user" : self.get_user_storage()})

        self.platforms = self.execute_local_query(query, annotation)

        platform_names_enabled = set([platform["platform"] for platform in self.platforms])
        platform_names_del     = platform_names_loaded - platform_names_enabled 
        platform_names_add     = platform_names_enabled - platform_names_loaded

        for platform_name in platform_names_del:
            # Unreference this platform which not more used
            Log.info("Disabling platform '%s'" % platform_name) 
            try:
                del self.gateways[platform_name] 
            except:
                Log.error("Cannot remove %s from %s" % (platform_name, self.gateways))

        for platform_name in platform_names_add: 
            # Create Gateway corresponding to the current Platform
            Log.info("Enabling platform '%s'" % platform_name) 
            gateway = self.make_gateway(platform_name)
            assert gateway, "Invalid Gateway create for platform '%s': %s" % (platform_name, gateway)
            self.gateways[platform_name] = gateway 

            # Load Announces related to this Platform
            announces = gateway.get_announces()
            assert isinstance(announces, list), "%s::get_announces() should return a list : %s (%s)" % (
                gateway.__class__.__name__,
                announces,
                type(announces)
            )
            self.announces[platform_name] = announces 

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
        self.make_gateways()

    @returns(dict)
    def get_account_config(self, platform_name, user):
        """
        Retrieve the Account of a given User on a given Platform.
        Args:
            platform_name: A String containing the name of the Platform.
            user: The User who executes the QueryPlan (None if anonymous).
        Returns:
            The corresponding dictionnary, None if no account found for
            this User and this Platform.
        """
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        if platform_name == STORAGE_NAMESPACE:
            return dict() 

        annotation = Annotation({"user" : self.get_user_storage()})

        # Retrieve the Platform having the name "platform_name" in the Storage
        try:
            platforms = self.execute_local_query(
                Query().get("platform").filter_by("platform", "=", platform_name),
                annotation,
            )
            platform_id = platforms[0]["platform_id"]
        except IndexError:
            Log.error("interface::get_account_config(): platform %s not found" % platform_name)
            return None

        # Retrieve the first Account having the name "platform_name" in the Storage
        try:
            account_configs = self.execute_local_query( 
                Query().get("account").filter_by("platform_id", "=", platform_id),
                annotation
            )
        except IndexError:
            Log.error("interface::get_account_config(): no account found for platform %s" % platform_name)
            return None

        # Convert the json string "config" into a python dictionnary
        num_accounts = len(accounts)
        if num_accounts > 0:
            if num_accounts > 1:
                Log.warning("Several accounts found for [%s]@%s: %s" % (user["email"], platform_name, accounts))
            account = accounts[0]
            account["config"] = json.loads(account["config"])
        else:
            account = None

        return account

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
                "capability" : list(),
            })
        return output

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
