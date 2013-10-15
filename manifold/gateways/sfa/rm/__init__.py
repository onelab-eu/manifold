#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to query a SFA Registry Manager. 
# http://www.opensfa.info/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

import traceback, tempfile
from types                                  import GeneratorType, StringTypes, ListType
from datetime                               import datetime

from twisted.internet                       import defer
from twisted.python.failure                 import Failure 

from sfa.storage.record                     import Record
from sfa.trust.certificate                  import Keypair, Certificate
from sfa.util.xrn                           import Xrn, get_authority, hrn_to_urn

from manifold.gateways.gateway              import Gateway 
from manifold.gateways.sfa                  import SFAGatewayCommon, DEMO_HOOKS
from manifold.gateways.sfa.user             import ADMIN_USER, is_user_admin 
from manifold.gateways.sfa.proxy            import SFAProxy
from manifold.gateways.sfa.rm.credential    import Credential 
from manifold.models                        import db
from manifold.models.platform               import Platform 
from manifold.operators                 	import LAST_RECORD
from manifold.util.log                      import Log
from manifold.util.type                     import accepts, returns 

class CredentialException:
    pass

class SFA_RMGateway(SFAGatewayCommon):

    from manifold.gateways.sfa.rm.methods.authority import Authority
    from manifold.gateways.sfa.rm.methods.slice     import Slice 
    from manifold.gateways.sfa.rm.methods.user      import User 

    METHOD_MAP = {
        "authority" : Authority,
        "user"      : User,
        "slice"     : Slice
    }

    def __init__(self, interface, platform, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the platform_configuration related to this Gateway.
                It may contains the following keys:
        """
        super(SFA_RMGateway, self).__init__(interface, platform, platform_config)
        platform_config = self.get_config()

        if not "registry" in platform_config:
            raise KeyError("'registry' is missing in platform_configuration: %s (%s)" % (platform_config, type(platform_config)))

    @returns(GeneratorType)
    def get_rms(self):
        """
        Returns:
            Allow to iterate on the Platform corresponding this RM.
        """
        platforms = db.query(Platform).filter(Platform.gateway_type == "sfa_rm").filter(Platform.platform == self.get_platform_name()).all()
        assert len(platforms) == 1
        for platform in platforms: 
            assert isinstance(platform, Platform), "Invalid platform = %s (%s)" % (platform, type(platform))
            yield platform

    @returns(StringTypes)
    def get_url(self):
        """
        Returns:
            A String instance containing the URL of the Ressource 
            Manager managed by this Gateway. 
        """
        return self.get_config()["registry"]

    @staticmethod
    def generate_slice_credential(slice_hrn, user_account_config):
        """
        Generate slice credential.
        Args:
            slice_hrn: A String containing a slice Human Readable Name.
                Example: "ple.upmc.myslice_demo"
            user_account_config: A dictionnary storing the account configuration related to
                the User and to the nested Platform managed by this Gateway.
        """
        Log.debug("Not yet implemented. Run delegation script in the meantime")
    
    @defer.inlineCallbacks
    def perform_query(self, user, user_account_config, query):
        """
        Perform a Query on this Gateway.
        Args:
            user: A dictionnary carrying a description of the User issuing the Query.
            user_account_config: A dictionnary storing the account configuration related to
                the User and to the nested Platform managed by this Gateway.
            query: The User's Query.
        Returns:
            The list of corresponding Records if any.
        """
        VALID_ACTIONS = ["get", "create", "update", "delete", "execute"]
        action = query.get_action()
        object = query.get_from()
        instance = SFA_RMGateway.METHOD_MAP[object](self) 

        # Dynamically import the appropriate package.
        # http://stackoverflow.com/questions/211100/pythons-import-doesnt-work-as-expected
        __import__("%s%s" % ("manifold.gateways.sfa.", object))

        if action not in VALID_ACTIONS: 
            failure = Failure("Invalid action (%s), not in {%s}" % (action, ", ".join(VALID_ACTIONS)))
            failure.raiseException()

        # Instanciate the appropriate method.
        # http://stackoverflow.com/questions/3061/calling-a-function-from-a-string-with-the-functions-name-in-python
        Log.tmp("Calling %s::%s" % (instance, action))
        try:
            method = getattr(instance, action)
        except Exception, e:
            print "EXC > ", e
        Log.tmp("method = %s" % method)
        ret = yield method(user, user_account_config, query)
        Log.tmp("<<<< perform_query: ret = %s" % ret)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def forward(self, query, callback, is_deferred = False, execute = True, user = None, user_account_config = None, format = "dict", receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean set to True if this Query is async.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            user_account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            receiver: The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        super(SFAGatewayCommon, self).forward(query, callback, is_deferred, execute, user, user_account_config, format, receiver)
        identifier = receiver.get_identifier() if receiver else None

        try:
            Gateway.start(user, user_account_config, query)
        except Exception, e:
            Log.error("Error while starting SFA_RMGateway")
            traceback.print_exc()
            Log.error(str(e))

        if not user:
            self.error(receiver, query, "No user specified, aborting")
            return

        user = user.__dict__

        try:
            assert query, "Cannot run gateway with not query associated: %s" % self.get_platform_name()
            self.debug = "debug" in query.params and query.params["debug"]

            # << bootstrap
            # Cache admin config
            # TODO this seems to be useless
            admin_account = self.get_account(ADMIN_USER["email"])
            if admin_account: admin_account_config = admin_account["config"]

            # Overwrite user config (reference & managed acccounts)
            user_account = self.get_account(user["email"])
            if user_account: user_account_config = user_account["config"]
            # >> bootstrap
 
            # If no user_account_config: failure
            if not user_account_config:
                self.send(LAST_RECORD, callback, identifier)
                self.error(receiver, query, "Account related to %s for platform %s not found" % (user["email"], self.get_platform_name()))
                return

            # Call the appropriate method (TODO pass Query object)
            # TODO create method should not return an empty list

            result = list()
            try:
                Log.tmp("1" * 80)
                print "call pq"
                result = yield self.perform_query(user, user_account_config, query)
                print " ============== perform query returned", result
            except Exception, e:
                Log.tmp("2" * 80)
                Log.tmp("auth_type = %s" % user_account["auth_type"])
                if user_account["auth_type"] == "managed":
                    Log.info("Need to manage")
                    self.manage(user, user_account_config, admin_account_config) 
                    Log.info("Account managed, we now rerun the query")
                    result = yield self.perform_query(user, user_account_config, query)
                else:
                    failure = Failure("Account not managed: user_account_config = %s / auth_type = %s" % (account_config, user_account["auth_type"]))
                    failure.raiseException()

            Log.tmp("3" * 80)
            # Rename fetched fields if necessary
#            if self.map_fields and query.get_from() in self.map_fields:
#                Rename(receiver, self.map_fields[query.get_from()])
            
            # Send Records to the From Node.
            Log.tmp("result = %s" % result)
            for row in result:
                self.send(row, callback, identifier)
            self.send(LAST_RECORD, callback, identifier)
            self.success(receiver, query)

        except Exception, e:
            traceback.print_exc()
            self.send(LAST_RECORD, callback, identifier)
            self.error(receiver, query, str(e))


    def check_cred(self):
        """
        Raises:
            A CredentialException if the User's Credential are not sufficient
            to run its SFA Query.
        """
        Log.info("Checking credentials (not yet implemented)")

    @returns(Credential)
    def _get_cred(self, user, user_account_config, type, target_hrn = None):
        """
        Args:
            user: A dictionnary carrying a description of the User issuing the Query.
            user_account_config: A dictionnary storing the account configuration related to
                the User and to the nested Platform managed by this Gateway.
            type: A String instance among {"user", "authority", "slice"}
            target_hrn: A String identifying the requested object. 
        """
        self.check_cred()
        self.get_cred(user, user_account_config, type, target_hrn)

    @returns(unicode)
    def get_cred(self, user, user_account_config, type, target_hrn = None):
        """
        Args:
            user: A dictionnary carrying a description of the User issuing the Query.
            user_account_config: A dictionnary storing the account configuration related to
                the User and to the nested Platform managed by this Gateway.
            type: A String instance among {"user", "authority", "slice"}
            target_hrn: A String identifying the requested object. 
        Returns:
            The corresponding Credential instance.
        """
        delegated  ='delegated_' if not is_user_admin(user) else ''
            
        if type == 'user':
            if target_hrn:
                raise Exception, "Cannot retrieve specific user credential for now"
            try:
                return user_account_config['%suser_credential' % delegated]
            except TypeError, e:
                raise Exception, "Missing user credential %s" % str(e)
        elif type == 'authority':
            if target_hrn:
                raise Exception, "Cannot retrieve specific authority credential for now"
            return user_account_config['%sauthority_credential'%delegated]
        elif type == 'slice':
            if not 'delegated_slice_credentials' in user_account_config:
                user_account_config['%sslice_credentials' % delegated] = {}

            creds = user_account_config['%sslice_credentials' % delegated]
            if target_hrn in creds:
                cred = creds[target_hrn]
            else:
                # Can we generate them : only if we have the user private key
                # Currently it is not possible to request for a slice credential
                # with a delegated user credential...
                if 'user_private_key' in user_account_config and account_config['user_private_key']:
                    cred = SFA_RMGateway.generate_slice_credential(target_hrn, user_account_config)
                    creds[target_hrn] = cred
                else:
                    raise Exception , "no cred found of type %s towards %s " % (type, target_hrn)
            return cred
        else:
            raise Exception, "Invalid credential type: %s" % type

    # LOIC: sfa_rm
    def delegate(self, user_credential, user_private_key, user_gid, admin_credential):
        """
        This function is used to delegate a user credential to the ADMIN_USER.
        Args:
            user_credential:
            user_private_key:
            user_gid:
            admin_credential:
        """

       # if nessecary converting string to Credential object
        if not isinstance (user_credential, Credential):
            user_credential = Credential(string=user_credential)
        # How to set_passphrase of the PEM key if we don't have the  user password?
        # For the moment we will use PEM keys without passphrase

        # does the user has the right to delegate all its privileges?
        if not user_credential.get_privileges().get_all_delegate():
            raise Exception, "SFA Gateway the user has no right to delegate"

        # if nessecary converting string to Credential object
        if not isinstance (admin_credential, Credential):
            admin_credential = Credential (string=admin_credential)
        # get the admin_gid and admin_hrn from the credential
        admin_gid = admin_credential.get_gid_object()
        admin_hrn = admin_gid.get_hrn()

        # Create temporary files for key and certificate in order to use existing code based on httplib 
        pkey_fn = tempfile.NamedTemporaryFile(delete=False) 
        pkey_fn.write(user_private_key.encode('latin1')) 
        cert_fn = tempfile.NamedTemporaryFile(delete=False) 
        cert_fn.write(user_gid) # We always use the GID 
        pkey_fn.close() 
        cert_fn.close() 
        delegated_credential = user_credential.delegate(admin_gid, pkey_fn.name, cert_fn.name)
        delegated_credential_str = delegated_credential.save_to_string(save_parents=True)

        os.unlink(pkey_fn.name) 
        os.unlink(cert_fn.name)
        return delegated_credential_str


    ############################################################################ 
    # ACCOUNT MANAGEMENT
    ############################################################################ 
    # using defer to have an asynchronous results management in functions prefixed by yield

    # TEST = PRESENT and NOT EXPIRED
    @returns(bool)
    def credentials_needed(self, cred_name, config):
        # TODO: optimize this function in the case that the user has no authority_credential and no slice_credential, it's executed each time !!!
        # Initialize
        need_credential = None

        # if cred_name is not defined in config, we need to get it from SFA Registry
        if not cred_name in config:
            need_credential = True
            #return True
        else:
            # testing if credential is empty in the DB
            if not config[cred_name]:
                need_credential = True
            else:
                # if config[cred_name] is a dict of credentials or a single credential
                if isinstance(config[cred_name], dict):
                    # check expiration of each credential
                    for cred in config[cred_name].values():
                        # if one of the credentials is expired, we need to get a new one from SFA Registry
                        if self.credential_expired(cred):
                            need_credential = True
                            #return True
                        else:
                            need_credential = False
                else:
                    # check expiration of the credential
                    need_credential = self.credential_expired(config[cred_name])
        # TODO: check all cases instead of tweaking like that
        if need_credential is None:
            need_credential = True
        return need_credential

    @returns(bool)
    def credential_expired(self, credential):
        """
        Tests whether a Credential has expired or not.
        Args:
            credential: A Credential or a String instance.
        Returns;
            True iif this Credential has expired.
        """
        #assert isinstance(credential, (str, Credential)), "Invalid Credential: %s (%s)" % (credential, type(credential))

        if not isinstance (credential, Credential):
            credential = Credential(string = credential)

        return credential.get_expiration() < datetime.now()
   

    # LOIC: sfa_rm
    @defer.inlineCallbacks
    def manage(self, user, user_account_config, admin_account_config):
        """
        This function is called for "managed" accounts. See in the Storage account.auth_type.
        Args:
            user: A dictionnary corresponding to the User.
            user_account_config: A dictionnary corresponding to account.config for this User and this Platform.
            admin_account_config: A dictionnary corresponding to account.config for the ADMIN_USER
        """
        # The gateway should be able to perform user user_account_config management taks on
        # behalf of MySlice
        #
        # FIELDS: 
        # - user_public_key
        # - user_private_key
        # - keypair_timestamp
        # - sscert
        # - user credentials (expiration!)
        # - gid (expiration!)
        # - slice credentials (expiration!)

        # Check fields that are present and credentials that are not expired
        # we will deduce the needed fields

        # The administrator is used as a mediator at the moment and thus does
        # not require any (delegated) credential. This could be modified in the
        # future if we expect MySlice to perform some operations on the testbed
        # under its name
        # NOTE We might want to manage a user account for direct use without
        # relying on delegated credentials. In this case we won't even have a
        # admin_account_config, and won't need delegation.
        is_admin = is_user_admin(user)

        # SFA management dependencies:
        #     U <- provided
        #    KP <- provided/generate
        #   SSC <- KP
        # proxy <- KP + SSC
        #    UC <- U + proxy (R:GetSelfCredential)          -- True if as_user
        #   GID <- proxy (GetGid)
        #    SL <- UC + proxy (R:List)
        #    AL <- U + get_authority(U)                     -- TODO clarify the different authority credentials
        #    SC <- UC + SL + proxy (R:GetCredential)        -- True if as_user
        #    AC <- UC + AL + proxy (R:GetCredential)        -- True if as_user
        #   DUC <- UC + K + GID + admin_U + admin_GID       -- True if !is_admin + !as_user and !OK
        #   DSC <- SC + K + GID + admin_U + admin_GID       -- True if !is_admin + !as_user and !OK
        #   DAC <- AC + K + GID + admin_U + admin_GID       -- True if !is_admin + !as_user and !OK
        # 
        # Legend:
        #  OK : present + !expired
        # X -> Y : X is used to get Y     proxy  : XMLRPC proxy
        #  KP : keypair                      SSC : self-signed certificate        GID : GID
        #   U : user hrn                      SL : slice list                      AL : authority list
        #  UC : user credential               SC : slice credentials               AC : authority credentials
        # DUC : delegated user credential    DSC : delegated slice credentials    DAC : delegated authority credentials
        # 
        # The order can be found using a reverse topological sort (tsort)
        # 
        need_delegated_slice_credentials = not is_admin and self.credentials_needed("delegated_slice_credentials", user_account_config)
        need_delegated_authority_credentials = not is_admin and self.credentials_needed("delegated_authority_credentials", user_account_config)
        need_slice_credentials = need_delegated_slice_credentials
        need_slice_list = need_slice_credentials
        need_authority_credentials = need_delegated_authority_credentials
        need_authority_list = need_authority_credentials
        need_delegated_user_credential = not is_admin and self.credentials_needed("delegated_user_credential", user_account_config)
        need_gid = not "gid" in user_account_config
        need_user_credential = need_authority_credentials or need_slice_list or need_slice_credentials or need_delegated_user_credential or need_gid

        if is_admin:
            need_delegated_user_credential      = False
            need_delegated_slice_credential     = False
            need_delegated_authority_credential = False

         # As need_gid is always True, need_sscert will be True
        #need_sscert = need_gid or need_user_credential
        need_sscert = True

        # As need_sscert is always True, need_user_private_key will be True
        #need_user_private_key = need_sscert or need_delegated_user_credential or need_delegated_slice_credentials or need_delegated_authority_credentials
        need_user_private_key = True

        # As need_user_private_key is always True, need_user_hrn will be True
        #need_user_hrn = need_user_private_key or need_auth_list or need_slice_list
        need_user_hrn = True
        
        if not "user_hrn" in user_account_config:
            Log.error("SFA_RMGateway::manage(): hrn required to manage authentication")
            # return using asynchronous defer
            defer.returnValue({})
            #return {}

        if not "user_private_key" in user_account_config:
            Log.info("Generating user private key for user '%s'" % user)
            k = Keypair(create = True)
            user_account_config["user_public_key"] = k.get_pubkey_string()
            user_account_config["user_private_key"] = k.as_pem()
            new_key = True

        if not "sscert" in user_account_config:
            Log.info("Generating self-signed certificate for user '%s'" % user)
            x = user_account_config["user_private_key"].encode("latin1")
            keypair = Keypair(string = x)
            self_signed = Certificate(subject = user_account_config["user_hrn"])
            self_signed.set_pubkey(keypair)
            self_signed.set_issuer(keypair, subject=user_account_config["user_hrn"].encode("latin1"))
            self_signed.sign()
            user_account_config["sscert"] = self_signed.save_to_string()

        # create an SFA connexion to Registry, using user_account_config
        
        timeout = self.get_timeout()
        registry_url = self.get_config()["registry"]
        registry_proxy = self.get_sfa_proxy(registry_url, user, user_account_config, "sscert", timeout)
        if need_user_credential and self.credentials_needed("user_credential", user_account_config):
            Log.debug("Requesting user credential for user %s" % user)
            try:
                user_account_config["user_credential"] = yield registry_proxy.GetSelfCredential(
                    user_account_config["sscert"],
                    user_account_config["user_hrn"],
                    "user"
                )
            except:
                # some urns hrns may replace non hierarchy delimiters "." with an "_" instead of escaping the "."
                hrn = Xrn(user_account_config["user_hrn"]).get_hrn().replace("\.", "_")
                try:
                    user_account_config["user_credential"] = yield registry_proxy.GetSelfCredential(
                        user_account_config["sscert"],
                        hrn,
                        "user"
                    )
                except Exception, e:
                    raise Exception("SFA_RMGateway::manage() could not retreive user from SFA Registry: %s" % e)

        # SFA call Resolve to get the GID and the slice_list
        if need_gid or need_slice_list:
            Log.debug("Generating GID for user %s" % user)
            records = yield registry_proxy.Resolve(
                user_account_config["user_hrn"].encode("latin1"),
                user_account_config["user_credential"]
            )

            if not records:
                raise RecordNotFound("hrn %s (%s) unknown to registry %s" % (user_account_config["user_hrn"], "user", registry_url))

            records = [record for record in records if record["type"] == "user"]
            record = records[0]
            user_account_config["gid"] = record["gid"]

            try:
                user_account_config["slice_list"] = record["reg-slices"]
            except Exception, e:
                Log.warning("User %s has no slices" % str(user_account_config["user_hrn"]))

        # delegate user_credential
        if need_delegated_user_credential:
            Log.debug("SFA delegate user cred %s" % user_account_config["user_hrn"])
            user_account_config["delegated_user_credential"] = self.delegate(
                user_account_config["user_credential"],
                user_account_config["user_private_key"],
                user_account_config["gid"],
                admin_account_config["user_credential"]
            )

        if need_authority_list: #and not "authority_list" in user_account_config:
            user_account_config["authority_list"] = [get_authority(user_account_config["user_hrn"])]
 
        # Get Authority credential for each authority of the authority_list
        if need_authority_credentials: #and not "authority_credentials" in user_account_config:
            Log.debug("Generating authority credentials for each authority")
            user_account_config["authority_credentials"] = {}
            try:
                for authority_hrn in user_account_config["authority_list"]:
                    user_account_config["authority_credentials"][authority_hrn] = yield registry_proxy.GetCredential(
                        user_account_config["user_credential"],
                        authority_hrn.encode("latin1"),
                        "authority"
                    )
            except:
                # No authority credential
                pass

        # XXX TODO Factorization of slice and authority operations
        # Get Slice credential for each slice of the slice_list 
        if need_slice_credentials: 
            Log.debug("Generating slice credentials for each slice of the user")
            user_account_config["slice_credentials"] = {}
            for slice_hrn in user_account_config["slice_list"]:
                # credential_string is temp, not delegated 
                user_account_config["slice_credentials"][slice_hrn] = yield registry_proxy.GetCredential(
                    user_account_config["user_credential"],
                    slice_hrn.encode("latin1"),
                    "slice"
                ) 
 
        if need_delegated_authority_credentials:
            Log.debug("Delegating authority credentials")
            user_account_config["delegated_authority_credentials"] = {}           
            for auth_name, auth_cred in user_account_config["authority_credentials"].items():
                user_account_config["delegated_authority_credentials"][auth_name] = self.delegate(
                    auth_cred,
                    user_account_config["user_private_key"],
                    user_account_config["gid"],
                    admin_account_config["user_credential"]
                )

        if need_delegated_slice_credentials:
            Log.debug("Delegating slice credentials")
            user_account_config["delegated_slice_credentials"] = {}
            for slice_hrn, slice_cred in user_account_config["slice_credentials"].items():
                user_account_config["delegated_slice_credentials"][slice_hrn] = self.delegate(
                    slice_cred,
                    user_account_config["user_private_key"],
                    user_account_config["gid"],
                    admin_account_config["user_credential"]
                )

        # return using asynchronous defer
        defer.returnValue(user_account_config)


