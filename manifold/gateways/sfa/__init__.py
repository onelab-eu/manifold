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

from types                             import StringTypes, GeneratorType
from twisted.internet                  import defer
from twisted.python.failure            import Failure

from manifold.core.exceptions          import ManifoldException, MissingCredentialException, ManagementException, NoAccountException, NoAdminAccountException, InternalException
from manifold.core.packet              import ErrorPacket # XXX not catching exceptions if we comment this
from manifold.core.query               import Query 
from manifold.core.record              import Record, Records
from manifold.gateways                 import Gateway
from manifold.gateways.sfa.proxy       import SFAProxy
from manifold.gateways.sfa.proxy_pool  import SFAProxyPool
from manifold.gateways.sfa.user        import ADMIN_USER
from manifold.operators.rename         import Rename
from manifold.util.log                 import Log
from manifold.util.type                import accepts, returns 

DEFAULT_TIMEOUT = 20
DEMO_HOOKS = ["demo"]

# The following class is not suffixed using "Gateway" to avoid that Manifold
# consider it as a valid Gateway. This a "virtual" class used to factorize
# code written in am/__init__.py and rm/__init__.py

class SFAGatewayCommon(Gateway):

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

        # Get User
        try:
            query_user = Query.get("user").filter_by("email", "=", user_email)
            user, = self._interface.execute_local_query(query_user)
        except Exception, e:
            raise ValueError("No Account found for User %s, Platform %s ignored: %s" % (user_email, platform_name, traceback.format_exc()))

        # Get Platform related to this RM/AM
        try:
            query_platform = Query.get("platform").filter_by("platform", "=", platform_name)
            platform, = self._interface.execute_local_query(query_platform)
        except Exception, e:
            raise ValueError("Platform %s not found: %s" % (platform_name, traceback.format_exc()))

        # Retrieve RMs (list of dict) related to this Gateway.
        # - if this is a SFA_RMGateway, this is the RM itself.
        # - if this is a SFA_AMGateway, this retrieve each RM related to this AM.
        rm_platforms = self.get_rms()

        # Get Accounts for this user on each related RM
        try:
            platform_ids = list([platform["platform_id"] for platform in rm_platforms])
            query_accounts = Query.get("account")\
                    .filter_by("user_id",     "=", user["user_id"])\
                    .filter_by("platform_id", "{", platform_ids)
            accounts = self._interface.execute_local_query(query_accounts)
        except Exception, e:
            if user_email == ADMIN_USER:
                raise NoAdminAccountException("Account(s) not found for user %s and platform %s: %s" % (user, platform, traceback.format_exc()))
            else:
                raise NoAccountException("Account(s) not found for user %s and platform %s: %s" % (user, platform, traceback.format_exc()))

        if len(accounts) == 0:
            if user_email == ADMIN_USER:
                raise NoAdminAccountException("No account found for User %s on those RMs: %s" % (
                    user_email,
                    [platform["platform"] for platform in rm_platforms])
                )
            else:
                raise NoAccountException("No account found for User %s on those RMs: %s" % (
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

    @returns(SFAProxy)
    def get_sfa_proxy_admin(self):
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
        print "get hrn"
        sfa_proxy = self.get_sfa_proxy_admin()
        print "sfa proxy", sfa_proxy
        assert sfa_proxy and isinstance(sfa_proxy, SFAProxy), "Invalid proxy: %s (%s)" % (sfa_proxy, type(sfa_proxy))
        print "asking version"
        sfa_proxy_version = yield sfa_proxy.get_cached_version()    
        print "version", sfa_proxy_version
        defer.returnValue(sfa_proxy_version["hrn"])

    #--------------------------------------------------------------------------
    # Gateway 
    #--------------------------------------------------------------------------

    def __init__(self, interface, platform, platform_config):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        assert isinstance(platform_config, dict), \
            "Invalid platform_config = %s (%s)" % (platform_config, type(platform_config))
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

    def get_object(self, table_name):
        """
        Retrieve the Object corresponding to a table_name.
        See manifold.gateways.sfa.rm.methods
        Args:
            table_name: A String among {"user", "slice", "authority"}.
        Returns:
            The corresponding RM_Object class. 
        """
        assert table_name in self.METHOD_MAP.keys(), \
            "Invalid table_name (%s). It should be in {%s}" % (
                table_name,
                ", ".join(SFA_RMGateway.METHOD_MAP.keys())
            )
        # XXX SFARMGateway -> self -- jordan
        return self.METHOD_MAP[table_name](self) 

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def handle_error(self, user, user_account):
        """
        This function when a Query has failed in its first attemp.
        """
        Log.warning("Using anonymous to access Manifold's Storage")

        user_account_config = user_account['config'] if user_account else None

        # Retrieve admin's config
        admin_account = self.get_account(ADMIN_USER["email"])
        admin_account_config = admin_account["config"] if admin_account else None
        assert isinstance(admin_account_config, dict), "Invalid admin_account_config"

        # Managing account
        user_account_config = yield self.manage(user, user_account_config, admin_account_config)

        # Update the Storage consequently
        self._interface.execute_local_query(
            Query.update("account")\
                .set({"config": json.dumps(user_account_config)})\
                .filter_by("user_id",     "=", user_account["user_id"])\
                .filter_by("platform_id", "=", user_account["platform_id"])
        )



    # This is in fact receive_query_impl
    # XXX Maybe this could be common to all gateway except we have callbacks
    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        
        query = packet.get_query()
        annotation = packet.get_annotation()
        user = annotation.get("user", None)
        user_email = user["email"]

        if not user:
            self.error(packet, "No user specified, aborting")
            return

        try:
            user_account = self.get_account(user_email)
        except ValueError, e:
            # get_account raises ValueError if not account is found
            # No account => we should ignore this platform
            # XXX gateways misconfiguratons should not make the whole query
            # fail, unless they are alone. This is up to the Receiver to
            # transform gateway errors into query warnings.
            self.error(packet, "Account related to %s for platform %s not found" % (user_email, self.get_platform_name()))
            return
            
        except AttributeError:
            # XXX what is attribute error. I don't understand how the error is
            # handled. If we are looking into the annotation, what is inside
            # should be priority, and not the storage content.
            user_account = annotation["account"]

        user_account_config = user_account["config"]
        message_header = "On platform %s, using account %s: " % (self.get_platform_name(), user_email)

        # Check parameters
        assert isinstance(user,  dict), \
            "%sCannot run %s with invalid user = %s (%s)" % (message_header, user, type(user))
        assert isinstance(user_account,  dict), \
            "%sInvalid user_account = %s (%s) for platform %s" % (message_header, user_account, type(user_account))
        assert isinstance(user,  dict), \
            "%sInvalid user_account_config = %s (%s)" % (message_header, user_account_config, type(user_account_config))
        assert isinstance(query, Query), \
            "Cannot run %s without query" % self.get_platform_name()

        self.debug = "debug" in query.params and query.params["debug"]

        # Retrieve user's config (managed acccounts)
        user_account = self.get_account(user_email)
        if user_account: user_account_config = user_account["config"]
        assert isinstance(user_account_config,  dict), "Invalid user_account_config"

        # If no user_account_config: failure
        if not user_account_config:
            Log.error("account not found")
            self.error(packet, "Account related to %s for platform %s not found" % (user_email, self.get_platform_name()))
            return

        # Insert a RENAME Node above this FROM Node if necessary.
        # XXX This is only for RM, not AM -- jordan
        if hasattr(self, 'get_object'):
            instance = self.get_object(query.get_from())
            aliases  = instance.get_aliases()
            if aliases:
                Log.warning("I don't think this properly recables everything")
                try:
                    Rename(self, aliases)
                except Exception, e:
                    print "EEE:", e

        # We know this function will return either through gw.records() or
        # GW.error() so there is no need to specify callbacks
        # XXX This is more like forwarding in the SFA domain

        # This is the first time we are running the query. Because we want to
        # handle failure the first time, to try to perform a manage operation,
        # we are specifying some callbacks
        #
        # sfa::receive_impl [CURRENT]
        #    sfa.rm::perform_query            => returns a deferred
        #       sfa.rm.methods.rm_object::get => returns a deferred
        Log.debug("Perform query", packet)
        print "perform query", packet
        # XXX Poourquoi faire perform_query alors qu'on a deja un objet
        d = self.perform_query(user, user_account_config, packet)
        d.addCallback(self.records)
        d.addErrback(self.on_first_error, user, user_account, packet)
        print "perform query done"

    # This should be merged with the previous function
    # We call twice get_object
    # Only the module import path differs between RM and AM
    # XXX Properly refactored, this is common to all gateways to do the
    # forwarding to objects, knowing that 1 query == 1 object... no joins at
    # this stage with the gateway
    def perform_query(self, user, user_account_config, packet):
        """
        Perform a Query on this Gateway.
        Args:
            user: A dictionnary carrying a description of the User issuing the Query.
            user_account_config: A dictionnary storing the account configuration related to
                the User and to the nested Platform managed by this Gateway.
            packet: A packet query
        Returns:
            The list of corresponding Records if any.
        """
        query = packet.get_query()
        print "perform query", query

        # XXX This would be done on top of the first encountered interface. -- # jordan
        # Check whether action is set to a valid value.
        VALID_ACTIONS = ["get", "create", "update", "delete", "execute"]
        action = query.get_action()
        if action not in VALID_ACTIONS: 
            failure = Failure("Invalid action (%s), not in {%s}" % (action, ", ".join(VALID_ACTIONS)))
            failure.raiseException()

        # Dynamically import the appropriate package.
        # http://stackoverflow.com/questions/211100/pythons-import-doesnt-work-as-expected
        table_name  = query.get_from()
        object_name = table_name # XXX let's use object and not table_name to be consistent

        print "object_name", object_name
        # XXX why do we need import when we have METHOD_MAP in AM and RM ? --
        # jordan
        # XXX This might fail if we send wrong metadata: raise internal error
        #module_name = "%s%s" % ("manifold.gateways.sfa.rm.objects.", table_name)
        #__import__(module_name)

        # Call the appropriate method.
        # http://stackoverflow.com/questions/3061/calling-a-function-from-a-string-with-the-functions-name-in-python
        instance = self.get_object(object_name)
        print "instance=", instance
        method = getattr(instance, action)
        print "method=", method

        # The deferred returned by this methods is being associated a
        # callback by the parent function
        # XXX Let's agree on what is passed to the object
        # From what i see where, it is somehow a disguised packet and annotation
        return method(user, user_account_config, packet)

    # XXX We could factor this function with on_first_error
    def on_next_errors(self, failure, user, user_account, packet):
        # We send the original error
        print "on next error"
        self.send(ErrorPacket.from_exception(e))  

    @defer.inlineCallbacks
    def on_first_error(self, failure, user, user_account, packet):
        """
        This function intercepts error packets caused by a query, when issued for the first time.
        This way, we are able to intercept errors we might solve by running manage.
        NOTE: This function could also be Gateway.receive[error].
        NOTE: we might only add this callback in case of a managed account.
        """
        # We might call the appropriate method (for instance User.get(...)) in
        # a first time without managing the user account. If it fails and if
        # this account is managed, then run managed and try to rerun the Query.
        # XXX Be careful not to run in an infinite loop: has the account be
        # managed recently ?
        
        # XXX If exceptions are triggered in errbacks like here, they seem not to be catched
        # eg. import missing for ErrorPacket or ManifoldException
        print "ON FIRST ERROR"

        query = packet.get_query()

        # Apply a error handler...
        ret = yield self.apply_error_handler(failure.value, user, user_account)
        if ret:
            # ...and exit the error handler.
            defer.returnValue(None)
        else:
            # Forward the error packet
            print "FW packet"
            error_packet = ErrorPacket.from_exception(failure.value)
            self.send(packet, error_packet)

        # We send the original error (last = True)
        # XXX Ideally this trap should protect the whole function
        # XXX Protect all error handlers with try:except

    #--------------------------------------------------------------------------
    # Error handlers
    #--------------------------------------------------------------------------

    CMD_ADD_ACCOUNT = """
    INSERT INTO %(namespace)s:account
        SET email      = '%(user_email)s',
            platform  = '%(platform_name)s',
            auth_type = '%(auth_type)s',
            config    = '%(config)s'
    """

    @defer.inlineCallbacks
    def handle_no_admin_account(self, user, user_account):
        defer.returnValue(False)
        try:
            admin_hrn     = 'ple.upmc.slicebrowser' # XXX
            user_email    = ADMIN_USER
            platform_name = ''
            aut_type      = 'managed'
            config        = '{"user_hrn": %(admin_hrn)s}'
            query_add_account = CMD_ADD_ACCOUNT % locals()
            ret = self._interface.execute_local_query(query_add_account)
            defer.returnValue(True)
        except: pass
        defer.returnValue(False)

    @defer.inlineCallbacks
    def handle_missing_credential(self, user, user_account):
        yield self.handle_error(user, user_account)
        self.perform_query(user, user_account_config, query)    \
            .addCallback(self.records)                          \
            .addErrback(self.on_next_errors, user, user_account, packet)
        defer.returnValue(True)

    @defer.inlineCallbacks
    def handle_no_account(self, user, user_account):
        print "CREATING ACCOUNT FOR USER ADMIN IF IT IS THE CASE"
        defer.returnValue(False)

    EXCEPTION_HANDLERS = {
        NoAdminAccountException    : handle_no_admin_account,
    }
    MANAGED_EXCEPTION_HANDLERS = {
        MissingCredentialException : handle_missing_credential,
        NoAccountException         : handle_no_account,
    }

    @defer.inlineCallbacks
    def apply_error_handler(self, exception, user, user_account):
        # Non managed handlers
        if exception in self.EXCEPTION_HANDLERS:
            handler = self.EXCEPTION_HANDLERS[exception]
            try:
                ret = yield handler(user, user_account)
                defer.returnValue(ret)
            except Exception, e:
                # In case of failure, we also inform the user by adding an
                # error packet before the original error packet.
                error_packet = ErrorPacket.from_exception(InternalException)
                error_packet.unset_last()
                self.send(packet, error_packet)
                defer.returnValue(False)

        # Managed handlers
        if user_account.get('auth_type', None) != "managed":
            defer.returnValue(False)
        if exception in self.MANAGED_EXCEPTION_HANDLERS:
            handler = self.MANAGED_EXCEPTION_HANDLERS[exception]
            try:
                ret = yield handler(user, user_account)
                defer.returnValue(ret)
            except Exception, e:
                # When management fails, we also inform the user by adding an
                # error packet before the original error packet.
                error_packet = ErrorPacket.from_exception(ManagementException)
                error_packet.unset_last()
                self.send(packet, error_packet)
                defer.returnValue(False)
