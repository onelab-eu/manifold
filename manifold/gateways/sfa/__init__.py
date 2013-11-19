#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to access to SFA (Slice Federated
# Architecture) 
# http://www.opensfa.info/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

import json, traceback

from types                                  import StringTypes, GeneratorType
from twisted.internet                       import defer

from manifold.core.query                    import Query 
from manifold.core.record                   import Record, Records, LastRecord
from manifold.gateways                      import Gateway
from manifold.gateways.sfa.proxy            import SFAProxy
from manifold.gateways.sfa.proxy_pool       import SFAProxyPool
from manifold.gateways.sfa.user             import ADMIN_USER
from manifold.operators.rename          	import Rename
from manifold.util.log                  	import Log
from manifold.util.type                 	import accepts, returns 

DEFAULT_TIMEOUT = 20
DEMO_HOOKS = ["demo"]

# The following class is not suffixed using "Gateway" to avoid that Manifold
# consider it as a valid Gateway. This a "virtual" class used to factorize
# code written in am/__init__.py and rm/__init__.py

class SFAGatewayCommon(Gateway):

    #--------------------------------------------------------------------------
    # Storage config
    #--------------------------------------------------------------------------

    @returns(dict)
    def get_user_storage(self):
        """
        Returns:
            A dictionnary the Manifold User used by SFAGateways to query the
            Manifold Storage (or None if using anonymous access)
        """
        Log.warning("Using anonymous to access Manifold's Storage")
        user_storage = None

    #--------------------------------------------------------------------------
    # User config
    #--------------------------------------------------------------------------

    @returns(GeneratorType)
    def get_rms(self, user):
        """
        Retrieve RMs related to this SFA Gateway.
        Args:
            user: A dictionnary describing the User issuing the Query.
        Returns:
            Allow to iterate on the Platform corresponding this RM.
        """
        raise Exception, "This method must be overloaded"

    @returns(list)
    def get_accounts(self, user_email):
        """
        Retrieve Account(s) of a User in the corresponding RM(s).
        An AM may be related to one more RM. A RM is related to exactly one RM (itself).
        Args:
            user_email: A String containing the User's email.
        Raises:
            ValueError: if Account is not found in the Manifold Storage.
            Exception: if the Platform is not found in the Manifold Storage.
        Returns:
            The list of corresponding Accounts dictionnaries.
        """
        assert isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))
        platform_name = self.get_platform_name()

        Log.warning("Using anonymous to access Manifold's Storage")
        user_storage = self.get_user_storage() 

        # Get User
        try:
            user, = self._interface.execute_local_query(Query.get("user").filter_by("email", "=", user_email))
        except Exception, e:
            raise ValueError("No Account found for User %s, Platform %s ignored: %s" % (user_email, platform_name, traceback.format_exc()))

        # Get Platform related to this RM/AM
        try:
            platform, = self._interface.execute_local_query( Query.get("platform").filter_by("platform", "=", platform_name))
        except Exception, e:
            raise Exception("Platform %s not found: %s" % (platform_name, traceback.format_exc()))

        # Retrieve RMs (list of dict) related to this Gateway.
        # - if this is a SFA_RMGateway, this is the RM itself.
        # - if this is a SFA_AMGateway, this retrieve each RM related to this AM.
        rm_platforms = self.get_rms()

        # Get Accounts for this user on each related RM
        try:
            platform_ids = list([platform["platform_id"] for platform in rm_platforms])
            accounts = self._interface.execute_local_query(
                Query.get("account")\
                    .filter_by("user_id",     "=", user["user_id"])\
                    .filter_by("platform_id", "{", platform_ids)
            )
        except Exception, e:
            raise Exception("Account(s) not found for user %s and platform %s: %s" % (user, platform, traceback.format_exc()))

        if len(accounts) == 0:
            raise ValueError("No account found for User %s on those RMs: %s" % (
                user_email,
                [platform["platform"] for platform in rm_platforms])
            )

        # Translate the accounts in a convenient format
        for account in accounts:
            account["config"] = json.loads(account["config"])

        return accounts

    @returns(dict)
    def get_account(self, user_email):
        """
        Retrieve an Account corresponding to a given user.
        Args:
            user_email: A String containing the User's email.
        Raises:
            ValueError: if Account is not found in the Manifold Storage.
            Exception: if the Platform is not found in the Manifold Storage.
        Returns:
            The dictionnary reprensenting the User's Account. 
        """
        assert isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))

        # Let traverse exceptions raised by get_accounts
        accounts = self.get_accounts(user_email)

        # Ok, we got at least one Account.
        if len(accounts) > 1:
            Log.warning("Several Accounts found %s" % accounts)
        account = accounts[0]
        Log.debug("Account found: %s" % account["config"]["user_hrn"])
        return account

    #--------------------------------------------------------------------------
    # Server 
    #--------------------------------------------------------------------------

    @returns(StringTypes)
    def get_url(self):
        """
        Returns:
            The URL of the SFA server related to this SFA Gateway.
        """
        raise Exception("This method must be overloaded")

    @returns(SFAProxy)
    def get_sfa_proxy_admin(self):
        """
        Returns:
            The SFAProxy using MySlice Admin account.
        """
        admin_config = self.get_account(ADMIN_USER["email"])["config"]

        sfa_proxy = self.get_sfa_proxy_impl(
            self.get_url(),
            ADMIN_USER,
            admin_config,
            "gid",
            self.get_timeout()
        )
        sfa_proxy.set_network_hrn(sfa_proxy.get_hrn())
        return sfa_proxy

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get_hrn(self):
        """
        Returns:
            A String instance containing the HRN (Human Readable Name)
            corresponding to the AM or RM managed by this Gateway.
        """
        sfa_proxy = self.get_sfa_proxy_admin()
        assert sfa_proxy and isinstance(sfa_proxy, SFAProxy), "Invalid proxy: %s (%s)" % (sfa_proxy, type(sfa_proxy))
        sfa_proxy_version = yield sfa_proxy.get_cached_version()    
        defer.returnValue(sfa_proxy_version["hrn"])

    #--------------------------------------------------------------------------
    # Gateway 
    #--------------------------------------------------------------------------

    def __init__(self, interface, platform, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(SFAGatewayCommon, self).__init__(interface, platform, platform_config)
        self.sfa_proxy_pool = SFAProxyPool()
        platform_config = self.get_config()
        
        # platform_config has always ["caller"]
        # Check the presence of mandatory fields, default others
        #if not "hashrequest" in platform_config:    
        #    platform_config["hashrequest"] = False
        #if not "protocol" in platform_config:
        #    platform_config["protocol"] = "xmlrpc"
        if not "verbose" in platform_config:
            platform_config["verbose"] = 0
        if not "debug" in platform_config:
            platform_config["debug"] = False
        if not "timeout" in platform_config:
            platform_config["timeout"] = DEFAULT_TIMEOUT

    @returns(SFAProxy)
    def get_sfa_proxy_impl(self, interface_url, user, account_config, cert_type, timeout, store_in_cached = True):
        """
        Retrieve a SFAProxy toward a given SFA interface (RM or AM).
        Args:
            interface_url: A String containing the URL of the SFA interface.
            user: A dictionnary describing the User issuing the SFA Query.
            account_config: A dictionnary describing the User's Account.
            cert_type: A String among "gid" and "sscert".
            timeout: The timeout (in seconds).
            store_in_cache: A boolean set to True if this SFAProxy must be
                stored in the SFAProxyPool or only returned by this function.
        Returns:
            The requested SFAProxy.
        """
        return self.sfa_proxy_pool.get(interface_url, user, account_config, cert_type, timeout, store_in_cached)

    @returns(int)
    def get_timeout(self):
        """
        Returns:
            The maximum delay (in seconds) before considering that we wont get an answer.
        """
        try:
            return self.get_config()["timeout"]
        except KeyError:
            return DEFAULT_TIMEOUT

#DEPRECATED|    @defer.inlineCallbacks
#DEPRECATED|    def forward(self, query, callback, is_deferred = False, execute = True, user = None, user_account_config = None, format = "dict", receiver = None):
#DEPRECATED|        """
#DEPRECATED|        Query handler.
#DEPRECATED|        Args:
#DEPRECATED|            query: A Query instance, reaching this Gateway.
#DEPRECATED|            callback: The function called to send this record. This callback is provided
#DEPRECATED|                most of time by a From Node.
#DEPRECATED|                Prototype : def callback(record)
#DEPRECATED|            is_deferred: A boolean set to True if this Query is async.
#DEPRECATED|            execute: A boolean set to True if the treatement requested in query
#DEPRECATED|                must be run or simply ignored.
#DEPRECATED|            user: The User issuing the Query.
#DEPRECATED|            user_account_config: A dictionnary containing the user's account config.
#DEPRECATED|                In pratice, this is the result of the following query (run on the Storage)
#DEPRECATED|                SELECT config FROM local:account WHERE user_id == user.user_id
#DEPRECATED|            receiver: The From Node running the Query or None. Its ResultValue will
#DEPRECATED|                be updated once the query has terminated.
#DEPRECATED|        Returns:
#DEPRECATED|            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
#DEPRECATED|            decorator. 
#DEPRECATED|        """

    @defer.inlineCallbacks
    def receive(self, packet):
        
        Gateway.receive(self, packet)

#DEPRECATED|        identifier = receiver.get_identifier() if receiver else None
#DEPRECATED|        super(SFAGatewayCommon, self).forward(query, callback, is_deferred, execute, user, user_account_config, format, receiver)
#DEPRECATED|
#DEPRECATED|        try:
#DEPRECATED|            Gateway.start(user, user_account_config, query)
#DEPRECATED|        except Exception, e:
#DEPRECATED|            Log.error("Error while starting SFA_RMGateway")
#DEPRECATED|            traceback.print_exc()
#DEPRECATED|            Log.error(str(e))


        query = packet.get_query()
        annotation = packet.get_annotation()
        user = annotation.get('user', None)

        if not user:
            print "no user"
            self.error(query, "No user specified, aborting")
            return

        user_email = user["email"]

        try:
            assert query, "Cannot run %s without query" % self.get_platform_name()
            self.debug = "debug" in query.params and query.params["debug"]

            # Retrieve user's config (managed acccounts)
            user_account = self.get_account(user_email)
            if user_account: user_account_config = user_account["config"]
            assert isinstance(user_account_config,  dict), "Invalid user_account_config"
 
            # If no user_account_config: failure
            if not user_account_config:
                self.send(LastRecord())
                print "account not found"
                self.error(query, "Account related to %s for platform %s not found" % (user_email, self.get_platform_name()))
                return

            # Call the appropriate method (for instance User.get(...)) in a first
            # time without managing the user account. If it fails and if this
            # account is managed, then run managed and try to rerun the Query.
            result = list()
            try:
                result = yield self.perform_query(user, user_account_config, query)
            except Exception, e:
                if self.handle_error(user, user_account):
                    result = yield self.perform_query(user, user_account_config, query)
                else:
                    failure = Failure("Account not managed: user_account_config = %s / auth_type = %s" % (account_config, user_account["auth_type"]))
                    failure.raiseException()
           
            # Insert a RENAME above this FROM Node if necessary.
            instance = self.get_object(query.get_from())
            aliases  = instance.get_aliases()
            if aliases:
                Rename(receiver, aliases)
                callback = receiver.get_callback()

            # Send Records to the From Node.
            for row in result:
                self.send(Record(row), callback, identifier)
            self.send(LastRecord())

        except Exception, e:
            Log.error(traceback.format_exc())
            self.send(LastRecord())
            print "exc", e
            self.error(query, str(e))


