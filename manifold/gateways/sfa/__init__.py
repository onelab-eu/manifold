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

#Aujourd'hui
#1 instance = 1 platform (R+AM)
#Demain
#1 instance = R,platform ou AM, platform

#slice a du sens pour les 2
#user, authority pour Registry
#resources pour AM

# Call manage() iif account.auth_type == "managed"
#   a relancer : a chaque fois qu'une slice est creee pour que myslice ait la procuration sur ce nouveau slice ou en cas d'expiration
# The user must delegate manually using sfi.py iif account.auth_type == "user" which update the storage and then allow to get account.config["delegated_user_credential"]

import sys, os, os.path, re, tempfile, itertools
import zlib, hashlib, BeautifulSoup, urllib
import json, signal, traceback

from datetime                               import datetime
from types                                  import StringTypes, GeneratorType
from twisted.internet                       import defer

from manifold.gateways.gateway              import Gateway
from manifold.gateways.sfa.proxy            import SFAProxy
from manifold.gateways.sfa.proxy_pool       import SFAProxyPool
from manifold.gateways.sfa.user             import ADMIN_USER, check_user 
from manifold.models                    	import db
from manifold.models.account                import Account
from manifold.models.platform           	import Platform 
from manifold.models.user               	import User
from manifold.operators                 	import LAST_RECORD
from manifold.operators.rename          	import Rename # move into sfa_rm
from manifold.util.log                  	import Log
from manifold.util.type                 	import accepts, returns 

from sfa.trust.credential               	import Credential
from sfa.trust.gid                      	import GID
from sfa.util.xrn                       	import Xrn, get_authority
from sfa.util.cache                     	import Cache
from sfa.client.client_helper           	import pg_users_arg, sfa_users_arg
from sfa.client.return_value            	import ReturnValue
from xmlrpclib                          	import DateTime

DEFAULT_TIMEOUT = 20
DEMO_HOOKS = ['demo']

import uuid
def unique_call_id():
    return uuid.uuid4().urn

# The following class is not suffixed using "Gateway" to avoid that Manifold
# consider it as a valid Gateway

class SFAGatewayCommon(Gateway):

    #--------------------------------------------------------------------------
    # User config
    #--------------------------------------------------------------------------

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

        # Get User
        try:
            user = db.query(User).filter(User.email == user_email).one()
            user = user.__dict__
        except Exception, e:
            raise ValueError("No Account found for User %s, Platform %s ignored" % (user_email, platform_name))

        # Get Platform related to this RM/AM
        try:
            platform = db.query(Platform).filter(Platform.platform == platform_name).one()
        except Exception, e:
            raise Exception("Platform %s not found" % platform_name)
        
        # Get Accounts for this user on each related RM
        accounts = db.query(Account)\
            .filter(Account.user_id == user["user_id"])\
            .filter(Account.platform_id.in_(tuple([platform.platform_id for platform in self.get_rms()])))\
            .all()

        if len(accounts) == 0:
            raise ValueError("No account found for User %s on those RMs: %s" % (user_email, [platform.platform for platform in self.get_rms()]))

        # Translate the accounts in a convenient format
        accounts = [account.__dict__ for account in accounts]
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

    @returns(SFAProxy)
    def get_server(self):
        """
        Returns:
            The SFAProxy using MySlice Admin account.
        """
        admin_config = self.get_account(ADMIN_USER["email"])["config"]

        sfa_proxy = self.get_sfa_proxy(
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
        server = self.get_server()
        assert server and isinstance(server, SFAProxy), "Invalid proxy: %s (%s)" % (server, type(server))
        server_version = yield self.get_cached_server_version(server)    
        defer.returnValue(server_version["hrn"])

    # TODO move in SFAProxy
    @defer.inlineCallbacks
    def get_cached_server_version(self, server):
        """
        Args:
            server: A SFAProxy instance.
        Returns:
            The version of the SFA server wrapped in this Gateway.
        """
        assert isinstance(server, SFAProxy), "(1) Invalid proxy: %s (%s)" % (server, type(server))
        version = None 

        # Check local cache first
        cache_key = server.get_interface() + "-version"
        cache = Cache()
        if cache:
            version = cache.get(cache_key)

        if not version: 
            result = yield server.GetVersion()
            code = result.get('code')
            if code:
                if code.get('geni_code') > 0:
                    raise Exception(result['output']) 
                version = ReturnValue.get_value(result)
            else:
                version = result
            # cache version for 20 minutes
            cache.add(cache_key, version, ttl = 60 * 20)

        defer.returnValue(version)

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
#    def get_sfa_proxy(self, interface_url, user, account_config, cert_type, timeout = DEFAULT_TIMEOUT, store_in_cached = True):
    def get_sfa_proxy(self, interface_url, user, account_config, cert_type, timeout, store_in_cached = True):
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

#    @defer.inlineCallbacks
#    # TODO config must be added and refers to account::config where user = and platform = 
#    def forward(self, query, callback, is_deferred = False, execute = True, user = None, account_config = None, format = "dict", receiver = None):
#        """
#        Query handler.
#        Args:
#            query: A Query instance, reaching this Gateway.
#            callback: The function called to send this record. This callback is provided
#                most of time by a From Node.
#                Prototype : def callback(record)
#            is_deferred: A boolean set to True if this Query is async.
#            execute: A boolean set to True if the treatement requested in query
#                must be run or simply ignored.
#            user: The User issuing the Query.
#            account_config: A dictionnary containing the user's account config.
#                In pratice, this is the result of the following query (run on the Storage)
#                SELECT config FROM local:account WHERE user_id == user.user_id
#            receiver : The From Node running the Query or None. Its ResultValue will
#                be updated once the query has terminated.
#        Returns:
#            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
#            decorator. 
#        """
#        super(SFAGatewayCommon, self).forward(query, callback, is_deferred, execute, user, account_config, format, receiver)
#        identifier = receiver.get_identifier() if receiver else None
#        Log.tmp("starting sfa: query = %s" % query)
#        try:
#            Gateway.start(user, account_config, query)
#        except Exception, e:
#            Log.error(str(e))
#
#        # Now, in the rest of the SFAGatewayCommon, user is a dictionnary
#        user = user.__dict__
#        Log.tmp("entering try" % query)
#
#        try:
#            assert query, "Cannot run gateway with not query associated: %s" % self.get_platform_name()
#            self.debug = 'debug' in query.params and query.params['debug']
#
#            # << bootstrap
#            # Cache admin config
#            # TODO this seems to be useless
#            Log.tmp("get account config %s" % ADMIN_USER)
#            admin_account_config = self.get_account(ADMIN_USER)["config"]
#            assert admin_account_config, "Could not retrieve admin config"
#            Log.info("forward: admin's config successfully fetched")
#
#            # Overwrite user config (reference & managed acccounts)
#            Log.tmp("get account config %s" % user["email"]) 
#            user_account_config = self.get_account(user)["config"]
#            if user_account_config:
#                account_config = user_account_config
#            Log.info("forward: user's config successfully fetched")
#            # >> bootstrap
# 
#            # If no account_config: failure
#            if not account_config:
#                self.send(LAST_RECORD, callback, identifier)
#                self.error(receiver, query, str(e))
#                return
#
#            # Call the appropriate method (TODO pass Query object)
#            Log.tmp("calling %s_%s" % (query.get_action(), query.get_from()))
#            # TODO create method should not return an empty list
#            try:
#                result = []
#                Log.tmp("%s_%s" % (query.get_action(), query.get_from()))
#                yield result
##                result = yield getattr(self, "%s_%s" % (query.get_action(), query.get_from()))(
##                    user,
##                    account_config,
##                    query.get_where(),
##                    query.get_params(),
##                    query.get_select()
##                )
#            except Exception:
#                self.manage(user, account_config)
##                Log.tmp("end manage, have to rerun the query")
##                result = yield getattr(self, "%s_%s" % (query.get_action(), query.get_from()))(
##                    user,
##                    account_config,
##                    query.get_where(),
##                    query.get_params(),
##                    query.get_select()
##                )
#
#            # Rename fetched fields if necessary
##            if self.map_fields and query.get_from() in self.map_fields:
##                Rename(receiver, self.map_fields[query.get_from()])
##            
#            # Send Records to the From Node.
#            for row in result:
#                self.send(row, callback, identifier)
#            self.send(LAST_RECORD, callback, identifier)
#            self.success(receiver, query)
#
#        except Exception, e:
#            traceback.print_exc()
#            self.send(LAST_RECORD, callback, identifier)
#            self.error(receiver, query, str(e))
#
#

