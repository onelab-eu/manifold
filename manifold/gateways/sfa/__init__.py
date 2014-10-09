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

from types                              import StringTypes, GeneratorType
from twisted.internet                   import defer
from twisted.python.failure             import Failure

from manifold.core.exceptions           import ManifoldException, MissingCredentialException, ManagementException, NoAccountException, NoAdminAccountException, ManifoldInternalException, MissingSSCertException, MissingGIDException
from manifold.core.packet               import ErrorPacket # XXX not catching exceptions if we comment this
from manifold.core.query                import Query 
from manifold.core.record               import Record, Records
from manifold.gateways                  import Gateway
from manifold.gateways.sfa.proxy        import SFAProxy
from manifold.gateways.sfa.proxy_pool   import SFAProxyPool
from manifold.gateways.sfa.user         import ADMIN_USER, ADMIN_USER_EMAIL
from manifold.operators.rename          import Rename
from manifold.util.log                  import Log
from manifold.util.predicate            import eq
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 

DEFAULT_TIMEOUT = 20
DEMO_HOOKS      = ["demo"]
MAX_RETRIES     = 3
# The following class is not suffixed using "Gateway" to avoid that Manifold
# consider it as a valid Gateway. This a "virtual" class used to factorize
# code written in am/__init__.py and rm/__init__.py

# XXX Gateway
USER_KEY = 'email'
PLATFORM_KEY = 'platform'

class SFAGatewayCommon(Gateway):

    #--------------------------------------------------------------------------
    # Constructor
    #--------------------------------------------------------------------------

    def __init__(self, router, platform, platform_config):
        """
        Constructor
        Args:
            router: The Manifold Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        assert isinstance(platform_config, dict), \
            "Invalid platform_config = %s (%s)" % (platform_config, type(platform_config))
        super(SFAGatewayCommon, self).__init__(router, platform, platform_config)
        self.sfa_proxy_pool = SFAProxyPool()
        platform_config = self.get_config()

        # XXX Need to document the fields in platform config
        
        if not "verbose" in platform_config:
            platform_config["verbose"] = 0

        if not "debug" in platform_config:
            platform_config["debug"] = False

        # timeout = The maximum delay (in seconds) before considering that we wont get an answer.
        if not "timeout" in platform_config:
            platform_config["timeout"] = DEFAULT_TIMEOUT

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    #--------------------------------------------------------------------------
    # Gateway 
    #--------------------------------------------------------------------------

    # XXX This should be common to all gateways !!
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
                ", ".join(self.METHOD_MAP.keys())
            )
        return self.METHOD_MAP[table_name](self) 

    #--------------------------------------------------------------------------
    # Account management
    #--------------------------------------------------------------------------
    #
    # SFA Gateways uses several accounts:
    # - accounts on a range of platforms, where the authentication is performed.
    # This is typically the different RMs an AM trusts (SFA_AM), or the current
    # RM (SFA_RM)
    # - accounts from different users
    #   . the user for retrieving credentials
    #   . the admin for SSL connection
    #
    #--------------------------------------------------------------------------

    # XXX This should be common to all gateways. By default, we return the local
    # platform only.
    @returns(GeneratorType)
    def get_account_platforms(self, user):
        """
        Retrieve RMs related to this SFA Gateway.
        Args:
            user: A dictionnary describing the User issuing the Query.
        Returns:
            Allow to iterate on the Platform corresponding this RM.
        """
        raise Exception, "This method must be overloaded"

    # XXX This should be common to all gateways
    @returns(dict)
    def get_account(self, user_email, platform_name = None):
        """
        Retrieve Account(s) of a User in the corresponding RM(s).
        An AM may be related to one more RM. A RM is related to exactly one RM (itself).
        Args:
            user_email: A String containing the User's email.
            platform_name : A String containing the Platform's name.
        Raises:
            ValueError: if Account is not found in the Manifold Storage.
            Exception: if the Platform is not found in the Manifold Storage.
        Returns:
            The list of corresponding Accounts dictionnaries.
        """

        if not platform_name:
            platform_name = self.get_platform_name()

        assert isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        # We need to add the select() clause since those *-query are not # well-managed yet
        query = Query.get('account')                    \
            .filter_by(USER_KEY,     eq, user_email)    \
            .filter_by(PLATFORM_KEY, eq, platform_name) \
            .select('auth_type', 'config')
        try:
            accounts = self._router.execute_local_query(query)
        except Exception, e:
            print "EEE", e
            exception_class = NoAdminAccountException if user_email == ADMIN_USER_EMAIL else NoAccountException
            raise exception_class("No account found for User %s on Platform %s" % (user_email, platform_name))

        if len(accounts) > 1:
            Log.warning("Several Accounts found %s" % accounts)
        account = accounts[0]

        config = account.get('config', None)
        if config:
            account['config'] = json.loads(config)
        else:
            account['config'] = {}

        return account
        
    def get_account_config(self, user_email, platform_name = None):
        """
        """
        admin_account = self.get_account(user_email, platform_name)
        return admin_account.get('config')

    def set_account_config(self, user_email, platform_name, account_config):
        """
        """
        assert isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))
        assert isinstance(platform_name, StringTypes),\
            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))

        query = Query.update('account')                  \
            .set({'config': json.dumps(account_config)}) \
            .filter_by(USER_KEY,     eq, user_email)     \
            .filter_by(PLATFORM_KEY, eq, platform_name)

        # Update the Storage consequently
        self._router.execute_local_query(query)

    #--------------------------------------------------------------------------
    # SFA specifics
    #--------------------------------------------------------------------------

    @returns(list)
    def get_rm_names(self):
        """
        """
        raise Exception("This method must be overloaded")


    @returns(StringTypes)
    def get_first_rm_name(self):
        rm_names = self.get_rm_names()
        if not rm_names:
            raise Exception, "No trusted RM found for this AM"
        return rm_names[0]

    @returns(StringTypes)
    def get_url(self):
        """
        Returns:
            The URL of the SFA server related to this SFA Gateway.
        """
        raise Exception("This method must be overloaded")

    #@returns(SFAProxy)
    def get_sfa_proxy(self, user_email, platform_name = None, cert_type = 'gid', config = None):
        """
        Returns:
            The SFAProxy using the account from User identified by its email
            (user_email) to the Platform platform_name.
        """
        if not config:
            # Can we get rid of this ? this is not compatible with caching
            # We pass config in manage since it is not stored in the database
            # each time we add a new token
            account = self.get_account(user_email, platform_name)
            config = account['config']

        # XXX Why do we need a pool of proxies
        sfa_proxy = self.sfa_proxy_pool.get(
            self.get_url(),
            user_email,
            config,
            cert_type,
            self.get_timeout(),
            True
        )
        sfa_proxy.set_network_hrn(sfa_proxy.get_hrn())
        return sfa_proxy

    @returns(SFAProxy)
    def get_sfa_proxy_admin(self, platform_name = None):
        """
        A proxy for user admin on the local platform.
        """
        return self.get_sfa_proxy(ADMIN_USER_EMAIL, platform_name)

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

    def get_timeout(self):
        return self.get_config().get('timeout', DEFAULT_TIMEOUT)

    #--------------------------------------------------------------------------
    # Packet forwarding 
    #--------------------------------------------------------------------------

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
        user_email = user.get('email') if user else None

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

# DEPRECATED |        # Insert a RENAME Node above this FROM Node if necessary.
# DEPRECATED |        instance = self.get_object(query.get_table_name())
# DEPRECATED |        aliases  = instance.get_aliases()
# DEPRECATED |        if aliases:
# DEPRECATED |            Log.warning("I don't think this properly recables everything")
# DEPRECATED |            try:
# DEPRECATED |                print "****** RENAME", aliases
# DEPRECATED |                Rename(self, aliases)
# DEPRECATED |            except Exception, e:
# DEPRECATED |                print "EEE:", e
# DEPRECATED |

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
        # XXX Poourquoi faire perform_query alors qu'on a deja un objet
        d = self.perform_query(user, user_account_config, packet)
        d.addCallback(self.records, packet)
        d.addErrback(self.on_query_error, user, user_account, packet, MAX_RETRIES)

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

        # XXX This would be done on top of the first encountered router. -- # jordan
        # Check whether action is set to a valid value.
        VALID_ACTIONS = ["get", "create", "update", "delete", "execute"]
        action = query.get_action()
        if action not in VALID_ACTIONS: 
            failure = Failure("Invalid action (%s), not in {%s}" % (action, ", ".join(VALID_ACTIONS)))
            failure.raiseException()

        # Dynamically import the appropriate package.
        # http://stackoverflow.com/questions/211100/pythons-import-doesnt-work-as-expected
        table_name  = query.get_table_name()
        object_name = table_name # XXX let's use object and not table_name to be consistent

        # XXX why do we need import when we have METHOD_MAP in AM and RM ? --
        # jordan
        # XXX This might fail if we send wrong metadata: raise internal error
        #module_name = "%s%s" % ("manifold.gateways.sfa.rm.objects.", table_name)
        #__import__(module_name)

        # Call the appropriate method.
        # http://stackoverflow.com/questions/3061/calling-a-function-from-a-string-with-the-functions-name-in-python
        instance = self.get_object(object_name)
        method = getattr(instance, action)

        # The deferred returned by this methods is being associated a
        # callback by the parent function
        # XXX Let's agree on what is passed to the object
        # From what i see where, it is somehow a disguised packet and annotation
        return method(user, user_account_config, query)

    #--------------------------------------------------------------------------
    # Error handling
    #--------------------------------------------------------------------------

    @defer.inlineCallbacks
    def on_query_error(self, failure, user, user_account, packet, retries):
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

        if retries > 0:
            # Apply a error handler...
            ret = yield self.apply_error_handler(packet, failure.value, user, user_account)
            if ret:
                # If successful, redo the query...
                d = self.perform_query(user, user_account, packet)
                d.addCallback(self.records)
                d.addErrback(self.on_query_error, user, user_account, packet, retries - 1)
                # ... and exit the error handler.
                defer.returnValue(None)

        # The default behaviour is to forward the error packet
        error_packet = ErrorPacket.from_exception(failure.value)
        self.send(packet, error_packet)

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
            auth_type      = 'managed'
            config        = '{"user_hrn": %(admin_hrn)s}'
            query_add_account = CMD_ADD_ACCOUNT % locals()
            ret = self._router.execute_local_query(query_add_account)
            defer.returnValue(True)
        except: pass
        defer.returnValue(False)

    @defer.inlineCallbacks
    def handle_missing_account_information(self, user, user_account):

        # We need to call manage on the platform used to issue queries
        platform_name = self.get_first_rm_name()
        gateway = self._router.get_interface(platform_name)
        ret = yield gateway.manage(user['email'])
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def handle_missing_admin_account_information(self, user, user_account):
        """
        user : NOT USED
        user_account : NOT USED
        """

        # We need to call manage on the platform used to issue queries
        platform_name = self.get_first_rm_name()
        gateway = self._router.get_interface(platform_name)
        ret = yield gateway.manage(ADMIN_USER_EMAIL)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def handle_no_account(self, user, user_account):
        print "CREATING ACCOUNT FOR USER ADMIN IF IT IS THE CASE"
        defer.returnValue(False)


    @defer.inlineCallbacks
    def apply_error_handler(self, packet, exception, user, user_account):
        EXCEPTION_HANDLERS = {
            # Errors related to the user account
            MissingCredentialException : self.handle_missing_account_information,
            # Other errors
            # Errors related to the admin account
            NoAdminAccountException    : self.handle_no_admin_account,
            MissingSSCertException     : self.handle_missing_admin_account_information,
            MissingGIDException        : self.handle_missing_admin_account_information,
            NoAccountException         : self.handle_no_account,
        }

        # Non managed handlers
        exception_class = exception.__class__
        if exception_class in EXCEPTION_HANDLERS:
            handler = EXCEPTION_HANDLERS[exception_class]
            try:
                ret = yield handler(user, user_account)
                defer.returnValue(ret)
            except Exception, e:
                # In case of failure, we also inform the user by adding an
                # error packet before the original error packet.
                error_packet = ErrorPacket.from_exception(ManifoldInternalException)
                error_packet.unset_last()
                self.send(packet, error_packet)
                defer.returnValue(False)

        # XXX To be checked in handlers
        #if user_account.get('auth_type', None) != "managed":
        #    defer.returnValue(False)

