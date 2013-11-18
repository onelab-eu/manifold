#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold Interface can be either a Manifold Router or
# a Manifold Forwarder. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json, traceback
from types                          import GeneratorType, StringTypes
from twisted.internet.defer         import Deferred

from manifold.gateways              import Gateway
from manifold.core.packet           import QueryPacket
from manifold.core.query            import Query
from manifold.core.query_plan       import QueryPlan
from manifold.core.receiver         import Receiver
from manifold.core.record           import Record
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.models.platform       import Platform
from manifold.models.user           import User
from manifold.policy                import Policy
from manifold.util.type             import accepts, returns 
from manifold.util.log              import Log
from manifold.gateways              import register_gateways

STORAGE_URL = 'sqlite:////var/myslice/db.sqlite?check_same_thread=False'

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

    def __init__(self, user_storage = None, allowed_capabilities = None):
        """
        Create an Interface instance.
        Args:
            user_storage: A User instance (used to access to the Manifold Storage) or None
                if the Storage can be accessed anonymously.
            allowed_capabilities: A Capabilities instance or None
        """
        # Register the list of Gateways
        Log.info("Registering gateways")
        register_gateways()

        # self.platforms is list(dict) where each dict describes a platform.
        # See platform table in the Storage.
        sqlalchemy_gw = Gateway.get("sqlalchemy")
        if not sqlalchemy_gw:
            raise Exception, "Cannot find sqlalchemy gateway, which is necessary for DBStorage module"
        storage_config = {"url" : STORAGE_URL}
        self.storage = sqlalchemy_gw(self, None, storage_config)

        self.platforms = list()
        self.user_storage = user_storage

        # self.allowed_capabilities is a Capabilities instance (or None)
        self.allowed_capabilities = allowed_capabilities

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

    @returns(GeneratorType)
    def get_platforms(self):
        """
        Returns:
            A Generator allowing to iterate on Platform managed by this Interface.
        """
        for platform in self.platforms:
            yield platform

    @returns(Record)
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
            if platform['platform'] == platform_name:
                return platform
        return None 

    @returns(list)
    def execute_local_query(self, query, annotation = None, error_message = None):
        """
        Execute a Query related to the Manifold Storage
        (ie any "local:*" object).
        Args:
            query: A Query. query.get_from() should start with "local:".
            annotation:
            error_message: A String containing the error_message that must
                be written in case of failure.
        Returns:
            A list of Records.            
        """
        receiver    = SyncReceiver()
        packet      = QueryPacket(query, annotation, receiver)

        self.storage.receive(packet)
        result_value = receiver.get_result_value()

        if not result_value.is_success():
            if not error_message:
                error_message = "Error executing local query: %s" % query
            raise Exception, error_message

        return result_value["value"]
        

#OBSOLETE|    def get_user_config(self, user, platform):
#OBSOLETE|        # all are dict
#OBSOLETE|
#OBSOLETE|        platform_name = platform['platform']
#OBSOLETE|        platform_id   = platform['platform_id']
#OBSOLETE|        user_id       = user['user_id']
#OBSOLETE|
#OBSOLETE|        auth_type = platform.get('auth_type', None)
#OBSOLETE|        if not auth_type:
#OBSOLETE|            Log.warning("'auth_type' is not set in platform = %s" % platform_name)
#OBSOLETE|            return None
#OBSOLETE|
#OBSOLETE|        # XXX platforms might have multiple auth types (like pam)
#OBSOLETE|        # XXX we should refer to storage
#OBSOLETE|
#OBSOLETE|        if auth_type in ["none", "default"]:
#OBSOLETE|            user_config = {}
#OBSOLETE|
#OBSOLETE|        # For default, take myslice account
#OBSOLETE|        elif auth_type == 'user':
#OBSOLETE|
#OBSOLETE|            
#OBSOLETE|            # User account information
#OBSOLETE|            query_accounts = Query.get('local:account').filter_by('user_id', '==', user_id).filter_by('platform_id', '==', platform_id)
#OBSOLETE|            accounts = self.execute_local_query(query_accounts)
#OBSOLETE|
#OBSOLETE|            if accounts:
#OBSOLETE|                account = accounts[0]
#OBSOLETE|                user_config = account.get('config', None)
#OBSOLETE|                if user_config:
#OBSOLETE|                    user_config = json.loads(user_config)
#OBSOLETE|
#OBSOLETE|                # XXX This should disappear with the merge with router-v2
#OBSOLETE|                if account['auth_type'] == 'reference':
#OBSOLETE|                    ref_platform_name = user_config['reference_platform']
#OBSOLETE|
#OBSOLETE|                    query_ref_platform = Query.get('local:platform').filter_by('platform', '==', ref_platform_name)
#OBSOLETE|                    ref_platforms = self.execute_local_query(query_ref_platform)
#OBSOLETE|                    if not ref_platforms:
#OBSOLETE|                        raise Exception, 'Cannot find reference platform %s for platform %s' % (platform_name, ref_platform_name)
#OBSOLETE|                    ref_platform = ref_platforms[0]
#OBSOLETE|
#OBSOLETE|                    query_ref_account = Query.get('local:account').filter_by('user_id', '==', user_id).filter_by('platform_id', '==', ref_platform['platform_id'])
#OBSOLETE|                    ref_accounts = self.execute_local_query(query_ref_account)
#OBSOLETE|                    if not ref_accounts:
#OBSOLETE|                        raise Exception, 'Cannot find account information for reference platform %s' % ref_platform_name
#OBSOLETE|                    ref_account = ref_accounts[0]
#OBSOLETE|
#OBSOLETE|                    user_config = ref_account.get('config', None)
#OBSOLETE|                    if user_config:
#OBSOLETE|                        user_config = json.loads(user_config)
#OBSOLETE|
#OBSOLETE|            else:
#OBSOLETE|                user_config = {}
#OBSOLETE|
#OBSOLETE|        else:
#OBSOLETE|            raise ValueError("This 'auth_type' not supported: %s" % auth_type)
#OBSOLETE|
#OBSOLETE|        return user_config

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
        platform_config = json.loads(platform["config"])
        args = [self, platform_name, platform_config]

        # Gateway is a plugin_factory
        gateway = Gateway.get(platform["gateway_type"])
        if not gateway:
            raise Exception, "Gateway not found: %s" % platform["gateway_type"]
        return gateway(*args)

    def make_gateways(self):
        """
        (Re)build Announces related to each enabled platforms
        Delete Announces related to disabled platforms
        """
        platform_names_loaded = set([platform["platform"] for platform in self.get_platforms()])

        query       = Query().get("platform").filter_by("disabled", "=", False)
        annotation = {"user" : self.get_user_storage()}

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
            announces = gateway.get_metadata()
            assert isinstance(announces, list), "%s::get_metadata() should return a list : %s (%s)" % (
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
        Retrieve the account of a give User on a given Platform.
        Args:
            platform_name: A String containing the name of the Platform.
            user: The User who executes the QueryPlan (None if anonymous).
        Returns:
            The corresponding dictionnary, None if no account found for
            this User and this Platform.
        """
        if platform_name == 'local':
            return {}

        annotation = {"user" : self.get_user_storage()}

        # Retrieve the Platform having the name "platform_name" in the Storage
        platforms = self.storage.execute(
            Query().get("platform").filter_by("platform", "=", platform_name),
            annotation,
        )
        platform_id = platforms[0]["platform_id"]

        # Retrieve the first Account having the name "platform_name" in the Storage
        account_configs = self.storage.execute(
            Query().get("account").filter_by("platform_id", "=", platform_id),
            annotation
        )
        account_config = json.loads(account_configs[0]["config"]) if len(account_configs) > 0 else None

        return account_config

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

        # Retrieve for each Platform involved in the QueryPlan the account(s)
        # corresponding to this User.
        platform_names = set()
        for from_node in query_plan.get_froms():
            platform_names.add(from_node.get_platform_name())
        
        account_configs = dict()
        for platform_name in platform_names:
            account_configs[platform_name] = self.get_account_config(platform_name, user)

        for from_node in query_plan.get_froms():
            platform_name = from_node.get_platform_name()
            if platform_name == 'local':
                gateway = self.storage
            else:
                gateway = self.get_gateway(platform_name)
            if gateway:
                from_node.set_gateway(gateway)
                from_node.set_user(user)
                from_node.set_account_config(account_configs[platform_name])
            else:
                raise Exception("Cannot instanciate all required Gateways")

#DEPRECATED#    @returns(list)
#DEPRECATED#    def get_metadata_objects(self):
#DEPRECATED#        """
#DEPRECATED#        Returns:
#DEPRECATED#            A list of dictionnaries describing each 3nf Tables.
#DEPRECATED#        """
#DEPRECATED#        table_dicts = dict() 
#DEPRECATED#
#DEPRECATED#        # XXX not generic
#DEPRECATED#        for table in self.g_3nf.graph.nodes():
#DEPRECATED#            # Ignore non parent tables
#DEPRECATED#            if not self.g_3nf.is_parent(table):
#DEPRECATED#                continue
#DEPRECATED#
#DEPRECATED#            table_name = table.get_name()
#DEPRECATED#
#DEPRECATED#            # We may have several table having the same name but related
#DEPRECATED#            # to two different platforms set.
#DEPRECATED#            fields = set() | table.get_fields()
#DEPRECATED#            for _, child in self.g_3nf.graph.out_edges(table):
#DEPRECATED#                if not child.get_name() == table_name:
#DEPRECATED#                    continue
#DEPRECATED#                fields |= child.get_fields()
#DEPRECATED#
#DEPRECATED#            # Build columns from fields
#DEPRECATED#            columns = table_dicts[table_name]["column"] if table_name in table_dicts.keys() else list()
#DEPRECATED#            for field in fields:
#DEPRECATED#                column = field.to_dict()
#DEPRECATED#                assert "name" in column.keys(), "Invalid field dict" # DEBUG
#DEPRECATED#                if column not in columns:
#DEPRECATED#                    columns.append(column)
#DEPRECATED#
#DEPRECATED#            keys = tuple(table.get_keys().one().get_field_names())
#DEPRECATED#
#DEPRECATED#            table_dicts[table_name] = {
#DEPRECATED#                "table"      : table_name,
#DEPRECATED#                "column"     : columns,
#DEPRECATED#                "key"        : keys,
#DEPRECATED#                "capability" : list(),
#DEPRECATED#            }
#DEPRECATED#        return table_dicts.values()

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

    def check_forward(self, query, annotation, receiver):
        """
        Checks whether parameters passed to Interface::forward() are well-formed.
        Args:
            See Interface::forward.
        """
        assert isinstance(query, Query), \
            "Invalid Query: %s (%s)" % (query, type(query))
        assert not annotation or isinstance(annotation, dict), \
            "Invalid annotation  = %s (%s)" % (annotation, type(annotation))
        assert not receiver or issubclass(type(receiver), Receiver) or not receiver.set_result_value,\
            "Invalid receiver = %s (%s)" % (receiver, type(receiver))

    @staticmethod
    def success(receiver, query, result_value = None):
        """
        Shorthand method when Interface::forward is successful.
        Args:
            receiver: A Receiver instance.
            query: A Query instance.
            result_value: A ResultValue or None that will be assigned to receiver.
        """
        assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
        if receiver:
            receiver.set_result_value(result_value)

    @staticmethod
    def error(receiver, query, description = ""):
        """
        Shorthand method when Interface::error is successful.
        Args:
            receiver: A Receiver instance.
            query: A Query instance.
            description: A String containing a customized error message.
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

    def send_result_value(self, query, result_value, annotation, is_deferred):
        # if Interface is_deferred  
        d = defer.Deferred() if is_deferred else None

        if not d:
            return result_value
        else:
            d.callback(result_value)
            return d
        
    def send(self, query, records, annotation, is_deferred):
        rv = ResultValue.get_success(records)
        return self.send_result_value(query, rv, annotation, is_deferred)

    def process_qp_results(self, query, records, annotation, query_plan):
        # Enforcing policy
        (decision, data) = self.policy.filter(query, records, annotation)
        if decision != Policy.ACCEPT:
            raise Exception, "Unknown decision from policy engine"

        description = query_plan.get_result_value_array()
        return ResultValue.get_result_value(records, description)

    def execute_query_plan(self, query, annotation, query_plan, is_deferred = False):
        records = query_plan.execute(is_deferred)
        if is_deferred:
            # results is a deferred
            records.addCallback(lambda records: self.process_qp_results(query, records, annotation, query_plan))
            return records # will be a result_value after the callback
        else:
            return self.process_qp_results(query, records, annotation, query_plan)

    def forward(self, query, annotation = None, receiver = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Basically we only handle local queries here. The other queries are in charge
        of the forward() method of the class inheriting Interface.
        Args:
            query: The user's Query.
            annotation: A dictionnary or None containing Query's annotation.
            receiver: An instance supporting the method set_result_value or None.
                receiver.set_result_value() will be called once the Query has terminated.
        Returns:
            A Deferred instance if the Query is async,
            None otherwise (see QueryPlan::execute())
        """
        if receiver:
            receiver.set_result_value(None)
        self.check_forward(query, annotation, receiver)

        # Enforcing policy
        (decision, data) = self.policy.filter(query, None, annotation)
        if decision == Policy.ACCEPT:
            pass
        elif decision == Policy.REWRITE:
            _query, _annotation = data
            if _query:
                query = _query
            if _annotation:
                annotation = _annotation
        elif decision == Policy.RECORDS:
            return self.send(query, data, annotation, is_deferred)
        elif decision in [Policy.DENIED, Policy.ERROR]:
            if decision == Policy.DENIED:
                data = ResultValue.get_error(ResultValue.FORBIDDEN)
            return self.send_result_value(query, data)
        else:
            raise Exception, "Unknown decision from policy engine"
        
        # if Interface is_deferred  
        Log.warning("Interface::forward: TODO: manage defer properly")
        d = None
        #d = Deferred() if is_deferred else None

        # Implements common functionalities = local queries, etc.
        namespace = None

        # Handling internal queries
        if ":" in query.get_from():
            namespace, table_name = query.get_from().rsplit(":", 2)

        if namespace == self.LOCAL_NAMESPACE:
# <<
#OBSOLETE|            if table_name in ['object', 'gateway']:
#OBSOLETE|                if table_name == 'object':
#OBSOLETE|                    records = self.get_metadata_objects()
#OBSOLETE|                elif table_name == "gateway":
#OBSOLETE|                    records = [{'name': name} for name in Gateway.list().keys()]
#OBSOLETE|                qp = QueryPlan()
#OBSOLETE|                qp.ast.from_table(query, records, key = None).selection(query.get_where()).projection(query.get_select())
#OBSOLETE|                Interface.success(receiver, query, result_value)
#OBSOLETE|                return self.execute_query_plan(query, annotation, qp, is_deferred)
#OBSOLETE|                
#OBSOLETE|            else:
#OBSOLETE|                query_storage = query.copy()
#OBSOLETE|                query_storage.object = table_name
#OBSOLETE|                records = self.storage.execute(query_storage, annotation)
#OBSOLETE|
#OBSOLETE|                if query_storage.get_from() == "platform" and query_storage.get_action() != "get":
#OBSOLETE|                    self.make_gateways()
#OBSOLETE|
#OBSOLETE|                Interface.success(receiver, query, result_value)
#OBSOLETE|                return self.send(query, records, annotation, is_deferred)
# ==
            if table_name == "object":
                list_objects = self.get_metadata_objects()
                qp = QueryPlan()
                qp.ast.from_table(query, list_objects, key = None).selection(query.get_where()).projection(query.get_select())
                Interface.success(receiver, query)
                return qp.execute(d, receiver)
            else:
                query_storage = query.copy()
                query_storage.object = table_name
                output = self.storage.execute(query_storage, annotation)
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
# >>
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
                Interface.success(query, receiver, result_value)
                return self.execute_query_plan(query, annotation, qp, is_deferred)

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
