#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to access to SFA (Slice Federated
# Architecture) 
# http://groups.geni.net/geni/wiki/GeniApi
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

import sys, os, os.path, re, tempfile, itertools
import zlib, hashlib, BeautifulSoup, urllib
import json, signal, traceback
from datetime                           import datetime
from lxml                               import etree
from StringIO                           import StringIO
from types                              import StringTypes, ListType
from twisted.internet                   import defer

from manifold.core.result_value         import ResultValue
from manifold.core.filter               import Filter
from manifold.gateways                  import Gateway
from manifold.gateways.sfa.rspecs.SFAv1 import SFAv1Parser # as Parser
from manifold.gateways.sfa.proxy        import SFAProxy, make_sfa_proxy
from manifold.models                    import db
from manifold.models.platform           import Platform 
from manifold.models.user               import User
from manifold.operators                 import LAST_RECORD
from manifold.operators.rename          import Rename
from manifold.util.log                  import Log
from manifold.util.misc                 import make_list
from manifold.util.predicate            import contains, eq, lt, le, included
from manifold.util.type                 import accepts, returns 

from sfa.trust.certificate              import Keypair, Certificate, set_passphrase
from sfa.trust.gid                      import GID
from sfa.trust.credential               import Credential
from sfa.util.xrn                       import Xrn, get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from sfa.util.config                    import Config
from sfa.util.version                   import version_core
from sfa.util.cache                     import Cache
from sfa.storage.record                 import Record
from sfa.rspecs.rspec                   import RSpec
from sfa.rspecs.version_manager         import VersionManager
from sfa.client.client_helper           import pg_users_arg, sfa_users_arg
from sfa.client.return_value            import ReturnValue
from xmlrpclib                          import DateTime

DEFAULT_TIMEOUT = 20
DEFAULT_TIMEOUT_GETVERSION = 5

class TimeOutException(Exception):
    pass

def timeout_callback(signum, frame):
    raise TimeOutException, "Command ran for too long"

# FOR DEBUG
def row2dict(row):
    d = {}
    for column in row.__table__.columns:
        d[column.name] = getattr(row, column.name)

    return d

ADMIN_USER = 'admin'
DEMO_HOOKS = ['demo']

xslt='''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="no"/>

<xsl:template match="/|comment()|processing-instruction()">
    <xsl:copy>
      <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="*">
    <xsl:element name="{local-name()}">
      <xsl:apply-templates select="@*|node()"/>
    </xsl:element>
</xsl:template>

<xsl:template match="@*">
    <xsl:attribute name="{local-name()}">
      <xsl:value-of select="."/>
    </xsl:attribute>
</xsl:template>
</xsl:stylesheet>
'''

xslt_doc=etree.parse(StringIO(xslt))
transform=etree.XSLT(xslt_doc)

def get_network_name(hostname):
    signal.signal(signal.SIGALRM, timeout_callback)
    signal.alarm(5)
    try:
        soup = BeautifulSoup.BeautifulSoup(urllib.urlopen("http://%s"%hostname))
        t = soup.title.string
        if ' |' in t:
            name = t[:t.rindex(' |')]
        else:
            name = t
    except Exception, e:
        Log.warning("Exception in get_network_name: %s" % e)
        name = None
    signal.alarm(0)
    return name

import uuid
def unique_call_id(): return uuid.uuid4().urn


class SFAGateway(Gateway):

    config_fields = [
        'user_credential',      # string representing a user_credential
        'slice_credentials',    # dictionary mapping a slice_hrn to the
                                # corresponding slice credential
        'authority_credential',
        'sscert',
        'user_private_key',
        'user_hrn',
        'gid'
    ]

################################################################################
# BEGIN SFA CODE
################################################################################

    # researcher == person ?
    map_slice_fields = {
        'last_updated'      : 'slice_last_updated',         # last_updated != last == checked,
        'geni_creator'      : 'slice_geni_creator',
        'node_ids'          : 'slice_node_ids',             # X This should be 'nodes.id' but we do not want IDs
        'reg-researchers'   : 'user.user_hrn',              # This should be 'users.hrn'
        'reg-urn'           : 'slice_urn',                  # slice_geni_urn ???
        'site_id'           : 'slice_site_id',              # X ID 
        'site'              : 'slice_site',                 # authority.hrn
        'authority'         : 'authority_hrn',              # isn't it the same ???
        'pointer'           : 'slice_pointer',              # X
        'instantiation'     : 'slice_instantiation',        # instanciation
        'max_nodes'         : 'slice_max_nodes',            # max nodes
        'person_ids'        : 'slice_person_ids',           # X users.ids
        'hrn'               : 'slice_hrn',                  # hrn
        'record_id'         : 'slice_record_id',            # X
        'gid'               : 'slice_gid',                  # gid
        'nodes'             : 'nodes',                      # nodes.hrn
        'peer_id'           : 'slice_peer_id',              # X
        'type'              : 'slice_type',                 # type ?
        'peer_authority'    : 'slice_peer_authority',       # ??
        'description'       : 'slice_description',          # description
        'expires'           : 'slice_expires',              # expires
        'persons'           : 'slice_persons',              # users.hrn
        'creator_person_id' : 'slice_creator_person_id',    # users.creator ?
        'PI'                : 'slice_pi',                   # users.pi ?
        'name'              : 'slice_name',                 # hrn
        #'slice_id'         : 'slice_id',
        'created'           : 'created',                    # first ?
        'url'               : 'slice_url',                  # url
        'peer_slice_id'     : 'slice_peer_slice_id',        # ?
        'geni_urn'          : 'slice_geni_urn',             # urn/hrn
        'slice_tag_ids'     : 'slice_tag_ids',              # tags
        'date_created'      : 'slice_date_created'          # first ?
    }

    map_user_fields = {
        'authority': 'authority_hrn',               # authority it belongs to
        'peer_authority': 'user_peer_authority',    # ?
        'hrn': 'user_hrn',                          # hrn
        'gid': 'user_gid',                          # gif
        'type': 'user_type',                        # type ???
        'last_updated': 'user_last_updated',        # last_updated
        'date_created': 'user_date_created',        # first
        'email': 'user_email',                      # email
        'first_name': 'user_first_name',            # first_name
        'last_name': 'user_last_name',              # last_name
        'phone': 'user_phone',                      # phone
        'keys': 'user_keys',                        # OBJ keys !!!
        'reg-slices': 'slice.slice_hrn',            # OBJ slices
        'reg-pi-authorities': 'pi_authorities',
    }

    map_authority_fields = {
        'hrn'               : 'authority_hrn',                  # hrn
        'PI'                : 'pi_users',
    }

    map_fields = {
        'slice': map_slice_fields,
        'user' : map_user_fields,
        'authority': map_authority_fields
    }

    #
    # Get various credential and spec files
    #
    # Establishes limiting conventions
    #   - conflates MAs and SAs
    #   - assumes last token in slice name is unique
    #
    # Bootstraps credentials
    #   - bootstrap user credential from self-signed certificate
    #   - bootstrap authority credential from user credential
    #   - bootstrap slice credential from user credential
    #
    @defer.inlineCallbacks
    def get_user_config(self, user_email):
        """
        Retrieve the user configuration on the platform managed by this
        SFAGateway using its email.
        Args:
            user_email: A String containing the User's email.
        """
        try:
            user = db.query(User).filter(User.email == user_email).one()
        except Exception, e:
            raise Exception, 'Missing user %s: %s' % (user_email, str(e))
        # Get platform
        platform = db.query(Platform).filter(Platform.platform == self.platform).one()
        
# XXX DEL #         # Get Admin config
# XXX DEL #         new_admin_config=self.get_user_config(admin,platform)
# XXX DEL #         self.admin_config=json.loads(new_admin_config)

        # Get account
        accounts = [a for a in user.accounts if a.platform == platform]
        if not accounts:
            Log.info("No account for user %s. Ignoring platform %s" % (user_email, platform.platform))
            defer.returnValue(None)
        else:
            account = accounts[0]

        new_user_config = None
        if account.auth_type == 'reference':
            ref_platform = json.loads(account.config)['reference_platform']
            ref_platform = db.query(Platform).filter(Platform.platform == ref_platform).one()
            ref_accounts = [a for a in user.accounts if a.platform == ref_platform]
            if not ref_accounts:
                raise Exception, "reference account does not exist"
            ref_account = ref_accounts[0]
            if ref_account.auth_type == 'managed':
                # call manage function for this managed user account to update it 
                # if the managed user account has only a private key, the credential will be retrieved 
                res_manage = yield self.manage(user.email, ref_platform, json.loads(ref_account.config))
                new_user_config = json.dumps(res_manage)
                # if the config retrieved is different from the config stored, we need to update it
                if new_user_config != ref_account.config:
                    ref_account.config = new_user_config # jo
                    db.add(ref_account)
                    db.commit()
            # if account is not managed, just add the config of the refered account
            else:
                new_user_config = ref_account.config
                
        elif account.auth_type == 'managed':
            # call manage function for a managed user account to update it 
            # if the managed user account has only a private key, the credential will be retrieved 
            res_manage = yield self.manage(user_email, self.platform, json.loads(account.config))
            new_user_config = json.dumps(res_manage)
            if account.config != new_user_config:
                account.config = new_user_config
                db.add(account)
                db.commit()

        # return using defer async
        if new_user_config:
            defer.returnValue(json.loads(new_user_config))
        else:
            defer.returnValue(json.loads(account.config))
        #return json.loads(new_user_config) if new_user_config else None

    
    @staticmethod
    @returns(bool)
    def is_admin(user):
        assert isinstance(user, (StringTypes, dict)), "Invalid user: %s (%s)" % (user, type(user))
        if isinstance(user, StringTypes):
            return user == ADMIN_USER
        else:
            return user["email"] == ADMIN_USER

    @defer.inlineCallbacks
    def get_cached_server_version(self, server):
        """
        Args:
            server: A SFAProxy instance  
        """
        assert isinstance(server, SFAProxy), "(1) Invalid proxy: %s (%s)" % (server, type(server))
        # check local cache first
        version = None 
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
            cache.add(cache_key, version, ttl= 60*20)

        # version as a property of the gateway instanciated, to be used in the parser
        self.version = version

        defer.returnValue(version)

    @defer.inlineCallbacks
    def get_interface_hrn(self, server):
        """
        Args:
            server: A Proxy instance  
        """
        assert isinstance(server, SFAProxy), "(2) Invalid proxy: %s (%s)" % (server, type(server))

        server_version = yield self.get_cached_server_version(server)    
        defer.returnValue(server_version['hrn'])
        
    ### resurrect this temporarily so we can support V1 aggregates for a while
    @defer.inlineCallbacks
    def server_supports_options_arg(self, server):
        """
        Returns true if server support the optional call_id arg, False otherwise. 
        """
        server_version = yield self.get_cached_server_version(server)
        # xxx need to rewrite this 
        # XXX added not server version to handle cases where GetVersion fails (jordan)
        if not server_version or int(server_version.get('geni_api')) >= 2:
            defer.returnValue(True)
            return 
        defer.returnValue(False)
        
    @defer.inlineCallbacks
    def server_supports_call_id_arg(self, server):
        server_version = yield self.get_cached_server_version(server)
        if 'sfa' in server_version and 'code_tag' in server_version:
            code_tag = server_version['code_tag']
            code_tag_parts = code_tag.split("-")
            version_parts = code_tag_parts[0].split(".")
            major, minor = version_parts[0], version_parts[1]
            rev = code_tag_parts[1]
            if int(major) == 1 and minor == 0 and build >= 22:
                defer.returnValue(True)
                return
            defer.returnValue(False)

    ### ois = options if supported
    # to be used in something like serverproxy.Method (arg1, arg2, *self.ois(api_options))
    @defer.inlineCallbacks
    def ois(self, server, option_dict):
        flag = yield self.server_supports_options_arg(server)
        if flag:
            defer.returnValue(option_dict)
        else:
            flag = yield self.server_supports_call_id_arg(server)
            if flag:
                defer.returnValue([unique_call_id()])
            else:
                defer.returnValue([])

#MANDO|UNUSED|    ### cis = call_id if supported - like ois
#MANDO|UNUSED|    @defer.inlineCallbacks
#MANDO|UNUSED|    def cis(self, server):
#MANDO|UNUSED|        flag = yield self.server_supports_call_id_arg(server)
#MANDO|UNUSED|        if flag:
#MANDO|UNUSED|            defer.returnValue([unique_call_id()])
#MANDO|UNUSED|        else:
#MANDO|UNUSED|            defer.returnValue([])
#MANDO|UNUSED|
#MANDO|UNUSED|    ############################################################################ 
#MANDO|UNUSED|    #
#MANDO|UNUSED|    # SFA Method wrappers
#MANDO|UNUSED|    #
#MANDO|UNUSED|    ############################################################################ 
#MANDO|UNUSED|
#MANDO|UNUSED|    def sfa_list_records(self, cred, hrns, record_type=None):
#MANDO|UNUSED|        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
#MANDO|UNUSED|            raise Exception('Wrong filter in sfa_list')
#MANDO|UNUSED|        records = self.registry.List(hrns, cred)
#MANDO|UNUSED|        if record_type:
#MANDO|UNUSED|            records = filter_records(record_type, records)
#MANDO|UNUSED|        return records
#MANDO|UNUSED|
#MANDO|UNUSED|    ########################################################################### 
#MANDO|UNUSED|    #
#MANDO|UNUSED|    # GETVERSION & RECURSIVE SCAN
#MANDO|UNUSED|    #
#MANDO|UNUSED|    ############################################################################ 
#MANDO|UNUSED|
#MANDO|UNUSED|    # All commands should take a registry/sliceapi as a parameter to allow for
#MANDO|UNUSED|    # more than one
#MANDO|UNUSED|
#MANDO|UNUSED|    # type IN (aggregate, registry, local)
#MANDO|UNUSED|    def sfa_get_version(self, type='aggregate', url=None):
#MANDO|UNUSED|        if url:
#MANDO|UNUSED|            return {}
#MANDO|UNUSED|
#MANDO|UNUSED|        if type == 'local':
#MANDO|UNUSED|            version=version_core()
#MANDO|UNUSED|        else:
#MANDO|UNUSED|            if type == 'registry':
#MANDO|UNUSED|                server = self.registry
#MANDO|UNUSED|            else:
#MANDO|UNUSED|                server = self.sliceapi
#MANDO|UNUSED|            result = server.GetVersion(timeout=DEFAULT_TIMEOUT_GETVERSION)
#MANDO|UNUSED|            version = ReturnValue.get_value(result)
#MANDO|UNUSED|        return version
#MANDO|UNUSED|
#MANDO|UNUSED|
#MANDO|UNUSED|    # scan from the given interfacs as entry points
#MANDO|UNUSED|    # XXX we should be able to run this in parallel
#MANDO|UNUSED|    # which ones are seen but do not reply
#MANDO|UNUSED|    # how to group emulab
#MANDO|UNUSED|    def sfa_get_version_rec(self, user_config, interfaces):
#MANDO|UNUSED|        output = []
#MANDO|UNUSED|        if not isinstance(interfaces,list):
#MANDO|UNUSED|            interfaces=[interfaces]
#MANDO|UNUSED|
#MANDO|UNUSED|        # add entry points right away using the interface uid's as a key
#MANDO|UNUSED|        to_scan=interfaces
#MANDO|UNUSED|        scanned=[]
#MANDO|UNUSED|        # keep on looping until we reach a fixed point
#MANDO|UNUSED|        while to_scan:
#MANDO|UNUSED|            for interface in to_scan:
#MANDO|UNUSED|
#MANDO|UNUSED|                # performing xmlrpc call
#MANDO|UNUSED|                print "D: Connecting to interface", interface
#MANDO|UNUSED|                server = make_sfa_proxy(interface, user_config, self.config['timeout'])
#MANDO|UNUSED|                try:
#MANDO|UNUSED|                    version = ReturnValue.get_value(server.GetVersion(timeout=DEFAULT_TIMEOUT_GETVERSION))
#MANDO|UNUSED|                except Exception, why:
#MANDO|UNUSED|                    import traceback
#MANDO|UNUSED|                    print "E: ", why
#MANDO|UNUSED|                    version = None
#MANDO|UNUSED|                    print traceback.print_exc()
#MANDO|UNUSED|
#MANDO|UNUSED|                if version:
#MANDO|UNUSED|                    output.append(version)
#MANDO|UNUSED|                    if 'peers' in version: 
#MANDO|UNUSED|                        for (next_name,next_url) in version['peers'].iteritems():
#MANDO|UNUSED|                            if not next_url in scanned:
#MANDO|UNUSED|                                to_scan.append(next_url)
#MANDO|UNUSED|                scanned.append(interface)
#MANDO|UNUSED|                to_scan.remove(interface)
#MANDO|UNUSED|        return output

# TODO Analyze version output
# {   'code_tag': '2.0-9',
#     'code_url': 'git://git.onelab.eu/sfa.git@sfa-2.0-9',
#     'geni_ad_rspec_versions': [   {   'extensions': [   'http://www.protogeni.net/resources/rspec/ext/flack/1',
#                                                         'http://www.planet-lab.org/resources/sfa/ext/planetlab/1'],
#                                       'namespace': 'http://www.geni.net/resources/rspec/3',
#                                       'schema': 'http://www.geni.net/resources/rspec/3/ad.xsd',
#                                       'type': 'GENI',
#                                       'version': '3'},
#                                   {   'extensions': [],
#                                       'namespace': None,
#                                       'schema': None,
#                                       'type': 'SFA',
#                                       'version': '1'},
#                                   {   'extensions': [   'http://www.protogeni.net/resources/rspec/ext/flack/1',
#                                                         'http://www.planet-lab.org/resources/sfa/ext/planetlab/1'],
#                                       'namespace': 'http://www.protogeni.net/resources/rspec/2',
#                                       'schema': 'http://www.protogeni.net/resources/rspec/2/ad.xsd',
#                                       'type': 'ProtoGENI',
#                                       'version': '2'}],
#     'geni_api': 2,
#     'geni_api_versions': {   '2': 'http://localhost:12347'},
#     'geni_request_rspec_versions': [   {   'extensions': [   'http://www.protogeni.net/resources/rspec/ext/flack/1',
#                                                              'http://www.planet-lab.org/resources/sfa/ext/planetlab/1'],
#                                            'namespace': 'http://www.geni.net/resources/rspec/3',
#                                            'schema': 'http://www.geni.net/resources/rspec/3/request.xsd',
#                                            'type': 'GENI',
#                                            'version': '3'},
#                                        {   'extensions': [],
#                                            'namespace': None,
#                                            'schema': None,
#                                            'type': 'SFA',
#                                            'version': '1'},
#                                        {   'extensions': [   'http://www.protogeni.net/resources/rspec/ext/flack/1',
#                                                              'http://www.planet-lab.org/resources/sfa/ext/planetlab/1'],
#                                            'namespace': 'http://www.protogeni.net/resources/rspec/2',
#                                            'schema': 'http://www.protogeni.net/resources/rspec/2/request.xsd',
#                                            'type': 'ProtoGENI',
#                                            'version': '2'}],
#     'hostname': 'www.planet-lab.eu',
#     'hrn': 'ple',
#     'interface': 'slicemgr',
#     'peers': {   'elc': 'http://www.emanicslab.org:12347',
#                  'plc': 'http://www.planet-lab.org:12347',
#                  'ple': 'http://www.planet-lab.eu:12346',
#                  'plj': 'http://www.planet-lab.jp:12347',
#                  'ppk': 'http://www.planet-lab.kr:12347'},
#     'sfa': 2,
#     'urn': 'urn:publicid:IDN+ple+authority+sa'} 

# REGISTRY

# {   'code_tag': '2.0-9',
#     'code_url': 'git://git.onelab.eu/sfa.git@sfa-2.0-9',
#     'hostname': 'www.planet-lab.eu',
#     'hrn': 'ple',
#     'interface': 'registry',
#     'peers': {   'elc': 'http://www.emanicslab.org:12345',
#                  'plc': 'http://www.planet-lab.org:12345',
#                  'plj': 'http://www.planet-lab.jp:12345',
#                  'ppk': 'http://www.planet-lab.kr:12345'},
#     'urn': 'urn:publicid:IDN++ple'}

# LOCAL

# {   'code_tag': '-',
#     'code_url': 'should-be-redefined-by-specfile',
#     'hostname': 'adreena'}



    ############################################################################ 
    #
    # RSPEC PARSING
    #
    ############################################################################ 

    @staticmethod
    def parse_sfa_rspec(rspec):
        parser = SFAv1Parser(rspec)
        return parser.to_dict(self.version)

    @staticmethod
    def build_sfa_rspec(slice_id, resources, leases):
        parser = SFAv1Parser(resources, leases)
        return parser.to_rspec(slice_id)


    ############################################################################ 
    #
    # COMMANDS
    #
    ############################################################################ 

    # get a delegated credential of a given type to a specific target
    # default allows the use of MySlice's own credentials
#MANDO|    def _get_cred(self, type, target=None):
#MANDO|        user = self.user
#MANDO|        user_config = self.user_config

    def _get_cred(self, user, user_config, type, target = None):
        delegated  ='delegated_' if not SFAGateway.is_admin(user) else ''
            
        if type == 'user':
            if target:
                raise Exception, "Cannot retrieve specific user credential for now"
            try:
                return user_config['%suser_credential' % delegated]
            except TypeError, e:
                raise Exception, "Missing user credential %s" % str(e)
        elif type == 'authority':
            if target:
                raise Exception, "Cannot retrieve specific authority credential for now"
            return user_config['%sauthority_credential'%delegated]
        elif type == 'slice':
            if not 'delegated_slice_credentials' in user_config:
                user_config['%sslice_credentials'%delegated] = {}

            creds = user_config['%sslice_credentials'%delegated]
            if target in creds:
                cred = creds[target]
            else:
                # Can we generate them : only if we have the user private key
                # Currently it is not possible to request for a slice credential
                # with a delegated user credential...
                if 'user_private_key' in user_config and user_config['user_private_key']:
                    cred = SFAGateway.generate_slice_credential(target, user_config)
                    creds[target] = cred
                else:
                    raise Exception , "no cred found of type %s towards %s " % (type, target)
            return cred
        else:
            raise Exception, "Invalid credential type: %s" % type

    @defer.inlineCallbacks
    def update_slice(self, user, user_config, filters, params, fields):
        if 'resource' not in params:
            raise Exception, "Update failed: nothing to update"

        # Keys
        if not filters.has_eq('slice_hrn'):
            raise Exception, 'Missing parameter: slice_hrn'
        slice_hrn = filters.get_eq('slice_hrn')
        slice_urn = hrn_to_urn(slice_hrn, 'slice')
        resources = params['resource'] if 'resource' in params else []
        leases = params['lease'] if 'lease' in params else []

        # Credentials
        user_cred = self._get_cred(user, user_config, 'user')
        slice_cred = self._get_cred(user, user_config, 'slice', slice_hrn)

        # We suppose resource
        rspec = SFAGateway.build_sfa_rspec(slice_urn, resources, leases)
        #print "BUILDING SFA RSPEC", rspec

        # Sliver attributes (tags) are ignored at the moment


        # We need to pass sufficient information to the aggregate so that it is
        # able to create user records: urn, (email) and keys for each user that
        # should be able to access the slice.

        # need to pass along user keys to the aggregate.
        # users = [
        #  { urn: urn:publicid:IDN+emulab.net+user+alice
        #    keys: [<ssh key A>, <ssh key B>]
        #  }]
        users = []
        # xxx Thierry 2012 sept. 21
        # contrary to what I was first thinking, calling Resolve with details=False does not yet work properly here
        # I am turning details=True on again on a - hopefully - temporary basis, just to get this whole thing to work again
        slice_records = yield self.registry.Resolve(slice_urn, [user_cred])
        # Due to a bug in the SFA implementation when Resolve requests are
        # forwarded, records are not filtered (Resolve received a list of xrns,
        # does not resolve its type, then issue queries to the local database
        # with the hrn only)
        #print "W: SFAWrap bug workaround"
        slice_records = Filter.from_dict({'type': 'slice'}).filter(slice_records)

        # slice_records = self.registry.Resolve(slice_urn, [self.my_credential_string], {'details':True})
        if slice_records and 'reg-researchers' in slice_records[0] and slice_records[0]['reg-researchers']:
            slice_record = slice_records[0]
            user_hrns = slice_record['reg-researchers']
            user_urns = [hrn_to_urn(hrn, 'user') for hrn in user_hrns]
            user_records = yield self.registry.Resolve(user_urns, [user_cred])
            server_version = yield self.get_cached_server_version(self.registry)
            if 'sfa' not in server_version:
                print "W: converting to pg rspec"
                users = pg_users_arg(user_records)
                rspec = RSpec(rspec)
                rspec.filter({'component_manager_id': server_version['urn']})
                rspec = RSpecConverter.to_pg_rspec(rspec.toxml(), content_type='request')
            else:
                users = sfa_users_arg(user_records, slice_record)
        # do not append users, keys, or slice tags. Anything
        # not contained in this request will be removed from the slice

        # CreateSliver has supported the options argument for a while now so it should
        # be safe to assume this server support it
        api_options = {}
        api_options ['append'] = False
        api_options ['call_id'] = unique_call_id()
        ois = yield self.ois(self.sliceapi, api_options)

        if self.version['geni_api'] == 2:
            # AM API v2
            result = yield self.sliceapi.CreateSliver(slice_urn, [slice_cred], rspec, users, ois)
        else:
            # AM API v3
            result = yield self.sliceapi.Allocate(slice_urn, [slice_cred], rspec, ois)
            result = yield self.sliceapi.Provision([slice_urn], [slice_cred], ois)

        manifest = ReturnValue.get_value(result)

        if not manifest:
            print "NO MANIFEST FROM", self.platform
            defer.returnValue([])
        else:
            print "GOT MANIFEST FROM", self.platform
        rsrc_leases = SFAGateway.parse_sfa_rspec(manifest)

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        #print "oK"
        #print "SLICE=", slice
        defer.returnValue([slice])

    # minimally check a key argument
    @staticmethod
    def check_ssh_key(self, key):
        good_ssh_key = r'^.*(?:ssh-dss|ssh-rsa)[ ]+[A-Za-z0-9+/=]+(?: .*)?$'
        return re.match(good_ssh_key, key, re.IGNORECASE)

    @staticmethod
    def create_record_from_params(type, params):
        record_dict = {}
        if type == 'slice':
            # This should be handled beforehand
            if 'slice_hrn' not in params or not params['slice_hrn']:
                raise Exception, "Must specify slice_hrn to create a slice"
            xrn = Xrn(params['slice_hrn'], type)
            record_dict['urn'] = xrn.get_urn()
            record_dict['hrn'] = xrn.get_hrn()
            record_dict['type'] = xrn.get_type()
        if 'key' in params and params['key']:
            #try:
            #    pubkey = open(params['key'], 'r').read()
            #except IOError:
            pubkey = params['key']
            if not SFAGateway.check_ssh_key(pubkey):
                raise SfaInvalidArgument(name='key',msg="Wrong key format")
                #raise SfaInvalidArgument(name='key',msg="Could not find file, or wrong key format")
            record_dict['keys'] = [pubkey]
        if 'slices' in params and params['slices']:
            record_dict['slices'] = params['slices']
        if 'researchers' in params and params['researchers']:
            # for slice: expecting a list of hrn
            record_dict['researcher'] = params['researchers']
        if 'email' in params and params['email']:
            record_dict['email'] = params['email']
        if 'pis' in params and params['pis']:
            record_dict['pi'] = params['pis']

        #slice: description

        # handle extra settings
        #record_dict.update(options.extras)

        return Record(dict=record_dict)
 
    def create_slice(self, user, user_config, filters, params, fields):

        # Get the slice name
        if not 'slice_hrn' in params:
            raise Exception, "Create slice requires a slice name"
        slice_hrn = params['slice_hrn']

        # Are we creating the slice on the right authority
        slice_auth = get_authority(slice_hrn)
        server_version = self.get_cached_server_version(self.registry)
        server_auth = server_version['hrn']
        if not slice_auth.startswith('%s.' % server_auth):
            print "I: Not requesting slice creation on %s for %s" % (server_auth, slice_hrn)
            return []
        print "I: Requesting slice creation on %s for %s" % (server_auth, slice_hrn)
        print "W: need to check slice is created under user authority"
        cred = self._get_cred(user, user_config, 'authority')
        record_dict = SFAGateway.create_record_from_params('slice', params)
        try:
            slice_gid = self.registry.Register(record_dict, cred)
        except Exception, e:
            print "E: %s" % e
        return []

 
    # This function will return information about a given network using SFA GetVersion call
    # Depending on the object Queried, if object is network then get_network is triggered by
    # result = getattr(self, "%s_%s" % (q.action, q.object))(local_filters, q.params, fields)
    @defer.inlineCallbacks
    def get_network(self, user, user_config, filters = None, params = None, fields = None):
        # Network (AM) 
        server = self.sliceapi
        version = yield self.get_cached_server_version(server)
        # Hardcoding the get network call until caching is implemented
        #if q.action == 'get' and q.object == 'network':
        #platforms = db.query(Platform).filter(Platform.disabled == False).all()
        #for p in platforms:
        #    print "########## platform = %s",p.platform
        #    result={'network_hrn': p.platform, 'network_name': p.platform_longname}
        #    #self.send({'network_hrn': p.platform, 'network_name': p.platform_longname})
        #for r in version:
        #    print r
        #Log.tmp(version)
        # forward what has been retrieved from the SFA GetVersion call
        #result=version
        output = {}
        # add these fields to match MySlice needs
        for k,v in version.items():
            if k=='hrn':
                output['network_hrn']=v
            if k=='testbed':
                output['network_name']=v
        #result={'network_hrn': version['hrn'], 'network_name': version['testbed']}
        defer.returnValue([output])

        #if version is not None:
        #    # add these fields to match MySlice needs
        #    for k,v in version.items():
        #        if k=='hrn':
        #            result['network_hrn']=v
        #        if k=='testbed':
        #            result['network_name']=v
        #    #result={'network_hrn': version['hrn'], 'network_name': version['testbed']}
        #    #print "SfaGateway::get_network() =",result
        #return [result]

    def get_slice_demo(self, user, user_config, filters, params, fields):
            print "W: Demo hook"
            s= {}
            s['slice_hrn'] = "ple.upmc.agent"
            s['slice_description'] = 'DEMO SLICE'

            if self.platform != 'ple':
                s['resources'] = []
                return [s]

            has_resources = False
            has_users = False

            subfields = []
            for of in fields:
                if of == 'resource' or of.startswith('resource.'):
                    subfields.append(of[9:])
                    has_resources = True
                if of == 'user' or of.startswith('user.'):
                    has_users = True
            if has_resources:
                rsrc_leases = self.get_resource_lease(user, user_config, {'slice_hrn': 'ple.upmc.agent'}, subfields)
                if not rsrc_leases:
                    raise Exception, 'get_resources failed!'
                s['resource'] = rsrc_leases['resource']
                s['lease'] = rsrc_leases['lease'] 
            if has_users:
                s['user'] = [{'person_hrn': 'myslice.demo'}]
            if self.debug:
                s['debug'] = rsrc_leases['debug']

            return [s]

    @defer.inlineCallbacks
    def get_object(self, user, user_config, object, object_hrn, filters, params, fields):
        """
        Args:
            user: A dictionnary containing a SFA user, which may contains
                _sa_instance_state : sqlalchemy.orm.state.InstanceState
                user_id  : An Integer identifying this User in the SQLAlchemy Storage 
                accounts : A list of Account (see manifold.models.account)
                password : A String containing the crypted password
                config   : A String containing the user's config. Example:
                    u\'{"firstname":"Jordan","lastname":"Auge","affiliation":"LIP6"}\'
                email    : A String containing the email address
            user_config: A dictionnary containing user's credentials:
                delegated_user_credential: A String containing its credentials
                user_hrn: A String containing the user's user_hr, (example: 'ple.upmc.jordan_auge')
            object: A String containing the name of the queried table (examples: 'user', 'slice', ...)
            object_hrn: A String (example: 'user_hrn')
            filters: WHERE clause related to the handled Manifold Query
            params: params related to the  handled Manifold Query 
            fields: SELECT clause related to the handled Manifold Query
        """

        # Let's find some additional information in filters in order to restrict our research
        object_name = make_list(filters.get_op(object_hrn, [eq, included]))
        auth_hrn = make_list(filters.get_op('authority_hrn', [eq, lt, le]))

        assert isinstance(self.registry, SFAProxy), "(3) Invalid proxy: %s (%s)" % (self.registry, type(self.registry))
        interface_hrn = yield self.get_interface_hrn(self.registry)

        # recursive: Should be based on jokers, eg. ple.upmc.*
        # resolve  : True: make resolve instead of list
        if object_name:
            # 0) given object name

            # If the objects are not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            object_name = [ on for on in object_name if on.startswith(interface_hrn)]
            if not object_name:
                defer.returnValue([])

            # Check for jokers ?
            stack     = object_name
            resolve   = True

        elif auth_hrn:
            # 2) given authority

            # If the authority is not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            if not auth_hrn.startswith(interface_hrn):
                defer.returnValue([])

            resolve   = False
            recursive = False
            stack = []
            for hrn in auth_hrn:
                if not '*' in hrn: # jokers ?
                    stack.append(hrn)
                else:
                    stack = [interface_hrn]
                    break

        else: # Nothing given
            resolve   = False
            recursive = True
            stack = [interface_hrn]
        
        # TODO: user's objects, use reg-researcher
        
        cred = self._get_cred(user, user_config, 'user')

        if resolve:
            stack = map(lambda x: hrn_to_urn(x, object), stack)
            _result,  = yield self.registry.Resolve(stack, cred, {'details': True})

            # XXX How to better handle DateTime XMLRPC types into the answer ?
            # XXX Shall we type the results like we do in CSV ?
            result = {}
            for k, v in _result.items():
                if isinstance(v, DateTime):
                    result[k] = str(v) # datetime.strptime(str(v), "%Y%m%dT%H:%M:%S") 
                else:
                    result[k] = v

            defer.returnValue([result])
        
        if len(stack) > 1:
            deferred_list = []
            while stack:
                auth_xrn = stack.pop()
                d = self.registry.List(auth_xrn, cred, {'recursive': recursive})
                deferred_list.append(d)
                    
            result = yield defer.DeferredList(deferred_list)

            output = []
            for (success, records) in result:
                if not success:
                    continue
                output.extend([r for r in records if r['type'] == object])
            defer.returnValue(output)

        else:
            auth_xrn = stack.pop()
            records = yield self.registry.List(auth_xrn, cred, {'recursive': recursive})
            records = [r for r in records if r['type'] == object]
            defer.returnValue(records)
        Log.tmp("ok")

    def get_slice(self, user, user_config, filters, params, fields):
#MANDO|        user = self.user
        if user["email"] in DEMO_HOOKS:
            defer.returnValue(self.get_slice_demo(user, user_config, filters, params, fields))
            return

        return self.get_object(user, user_config, 'slice', 'slice_hrn', filters, params, fields)

    def get_user(self, user, user_config, filters, params, fields):
#MANDO|        user = self.user
        if user["email"] in DEMO_HOOKS:
            defer.returnValue(self.get_user_demo(user, user_config, filters, params, fields))
            return

        Log.tmp("calling get_object")
        return self.get_object(user, user_config, 'user', 'user_hrn', filters, params, fields)

    def get_authority(self, user, user_config, filters, params, fields):
#MANDO|        user = self.user
        #if user["email"] in DEMO_HOOKS:
        #    defer.returnValue(self.get_authority_demo(filters, params, fields))
        #    return

        return self.get_object(user, user_config, 'authority', 'authority_hrn', filters, params, fields)


# WORKING #        if len(stack) > 1:
# WORKING #            d = defer.Deferred()
# WORKING #            deferred_list = []
# WORKING #            while stack:
# WORKING #                auth_xrn = stack.pop()
# WORKING #                deferred_list.append(self.registry.List(auth_xrn, cred, {'recursive': recursive}))
# WORKING #            defer.DeferredList(deferred_list).addCallback(get_slice_callback).chainDeferred(d)
# WORKING #            return d
# WORKING #        else:
# WORKING #            auth_xrn = stack.pop()
# WORKING #            return self.registry.List(auth_xrn, cred, {'recursive': recursive})
# WORKING #
# WORKING #        def get_slice_callback(result):
# WORKING #            output = []
# WORKING #            for (success, records) in result:
# WORKING #                if not success:
# WORKING #                    print "ERROR in CALLBACK", records
# WORKING #                    continue
# WORKING #                output.extend([r for r in records if r['type'] == 'slice'])
# WORKING #            return output

# REFERENCE # This code is known to crash sfawrap, saved for further reference
# REFERENCE # 
# REFERENCE #         def get_slice_callback(result):
# REFERENCE #             try:
# REFERENCE #                 from twisted.internet import defer
# REFERENCE #                 for (success, records) in result:
# REFERENCE #                     if not success:
# REFERENCE #                         print "ERROR in CALLBACK", records
# REFERENCE #                         continue
# REFERENCE #                     for record in records:
# REFERENCE #                         if (record['type'] == 'slice'):
# REFERENCE #                             result.append(record)
# REFERENCE #                         elif (record['type'] == 'authority'):
# REFERENCE #                             # "recursion"
# REFERENCE #                             stack.append(record['hrn'])
# REFERENCE #                 deferred_list = []
# REFERENCE #                 if stack:
# REFERENCE #                     while stack:
# REFERENCE #                         auth_xrn = stack.pop()
# REFERENCE #                         deferred_list.append(self.registry.List(auth_xrn, cred, {'recursive': True}))
# REFERENCE #                     dl = defer.DeferredList(deferred_list).addCallback(get_slice_callback)
# REFERENCE #             except Exception, e:
# REFERENCE #                 print "E: get_slice_callback", e
# REFERENCE #                 import traceback
# REFERENCE #                 traceback.print_exc()


#DEPRECATED#    def get_user(self, filters = None, params = None, fields = None):
#DEPRECATED#
#DEPRECATED#        cred = self._get_cred(user, user_config, 'user')
#DEPRECATED#
#DEPRECATED#        # A/ List users
#DEPRECATED#        if not filters or not (filters.has_eq('user_hrn') or filters.has_eq('authority_hrn')):
#DEPRECATED#            # no authority specified, we get all users *recursively*
#DEPRECATED#            raise Exception, "E: Recursive user listing not implemented yet."
#DEPRECATED#
#DEPRECATED#        elif filters.has_eq('authority_hrn'):
#DEPRECATED#            # Get the list of users
#DEPRECATED#            auths = filters.get_eq('authority_hrn')
#DEPRECATED#            if not isinstance(auths, list): auths = [auths]
#DEPRECATED#
#DEPRECATED#            # Get the list of user_hrn
#DEPRECATED#            user_list = []
#DEPRECATED#            for hrn in auths:
#DEPRECATED#                ul = self.registry.List(hrn, cred)
#DEPRECATED#                ul = filter_records('user', ul)
#DEPRECATED#                user_list.extend([r['hrn'] for r in ul])
#DEPRECATED#
#DEPRECATED#        else: # named users
#DEPRECATED#            user_list = filters.get_eq('user_hrn')
#DEPRECATED#            if not isinstance(user_list, list): user_list = [user_list]
#DEPRECATED#        
#DEPRECATED#        if not user_list: return user_list
#DEPRECATED#
#DEPRECATED#        # B/ Get user information
#DEPRECATED#        if filters == set(['user_hrn']): # urn ?
#DEPRECATED#            return [ {'user_hrn': hrn} for hrn in user_list ]
#DEPRECATED#
#DEPRECATED#        else:
#DEPRECATED#            # Here we could filter by authority if possible
#DEPRECATED#            if filters.has_eq('authority_hrn'):
#DEPRECATED#                predicates = filters.get_predicates('authority_hrn')
#DEPRECATED#                for p in predicates:
#DEPRECATED#                    user_list = [s for s in user_list if p.match({'authority_hrn': get_authority(s)})]
#DEPRECATED#
#DEPRECATED#            if not user_list: return user_list
#DEPRECATED#
#DEPRECATED#            users = self.registry.Resolve(user_list, cred)
#DEPRECATED#            users = filter_records('user', users)
#DEPRECATED#            filtered = []
#DEPRECATED#
#DEPRECATED#            for user in users:
#DEPRECATED#                # translate field names...
#DEPRECATED#                for k,v in self.map_user_fields.items():
#DEPRECATED#                    if k in user:
#DEPRECATED#                        user[v] = user[k]
#DEPRECATED#                        del user[k]
#DEPRECATED#                # apply input_filters XXX TODO sort limit offset
#DEPRECATED#                if filters.match(user):
#DEPRECATED#                    # apply output_fields
#DEPRECATED#                    c = {}
#DEPRECATED#                    for k,v in user.items():
#DEPRECATED#                        if k in fields:
#DEPRECATED#                            c[k] = v
#DEPRECATED#                    filtered.append(c)
#DEPRECATED#
#DEPRECATED#            return filtered


#MANDO|UNUSED|    def sfa_table_networks(self):
#MANDO|UNUSED|        versions = self.sfa_get_version_rec(self.sm_url)
#MANDO|UNUSED|
#MANDO|UNUSED|        output = []
#MANDO|UNUSED|        for v in versions:
#MANDO|UNUSED|            hrn = v['hrn']
#MANDO|UNUSED|            networks = [x for x in output if x['network_hrn'] == hrn]
#MANDO|UNUSED|            if networks:
#MANDO|UNUSED|                print "I: %s exists!" % hrn
#MANDO|UNUSED|                continue
#MANDO|UNUSED|
#MANDO|UNUSED|            # XXX we might make temporary patches for ppk for example
#MANDO|UNUSED|            if 'hostname' in v and v['hostname'] == 'ppkplc.kaist.ac.kr':
#MANDO|UNUSED|                print "[FIXME] Hardcoded hrn value for PPK"
#MANDO|UNUSED|                v['hrn'] = 'ppk'
#MANDO|UNUSED|
#MANDO|UNUSED|            # We skip networks that do not advertise their hrn
#MANDO|UNUSED|            # XXX TODO issue warning
#MANDO|UNUSED|            if 'hrn' not in v:
#MANDO|UNUSED|                continue
#MANDO|UNUSED|
#MANDO|UNUSED|            # Case when hostnames differ
#MANDO|UNUSED|
#MANDO|UNUSED|            network = {'network_hrn': v['hrn']}
#MANDO|UNUSED|            # Network name
#MANDO|UNUSED|            #name = None
#MANDO|UNUSED|            #if 'hostname' in version:
#MANDO|UNUSED|            #    name = get_network_name(version['hostname'])
#MANDO|UNUSED|            #if not name:
#MANDO|UNUSED|            #    name = get_network_name(i.hostname)
#MANDO|UNUSED|            #if name:
#MANDO|UNUSED|            #    network['network_name'] = name
#MANDO|UNUSED|            output.append(network)
#MANDO|UNUSED|
#MANDO|UNUSED|        # same with registries
#MANDO|UNUSED|        return output
#MANDO|UNUSED|
#MANDO|UNUSED|    def get_networks(self, input_filter = None, output_fields = None):
#MANDO|UNUSED|        networks = self.sfa_table_networks()
#MANDO|UNUSED|        return project_select_and_rename_fields(networks, 'network_hrn', input_filter, output_fields)
#MANDO|UNUSED|
#MANDO|UNUSED|    def get_recauth(self, input_filter = None, output_fields = None):
#MANDO|UNUSED|        user_cred = self.get_user_cred().save_to_string(save_parents=True)
#MANDO|UNUSED|        records = self.get_networks()
#MANDO|UNUSED|        todo = [r['network_hrn'] for r in records]
#MANDO|UNUSED|        while todo:
#MANDO|UNUSED|            newtodo = []
#MANDO|UNUSED|            for hrn in todo:
#MANDO|UNUSED|                try:
#MANDO|UNUSED|                    records = self.registry.List(hrn, user_cred)
#MANDO|UNUSED|                except Exception, why:
#MANDO|UNUSED|                    print "Exception during %s: %s" % (hrn, str(why))
#MANDO|UNUSED|                    continue
#MANDO|UNUSED|                records = filter_records('authority', records)
#MANDO|UNUSED|                newtodo.extend([r['hrn'] for r in records])
#MANDO|UNUSED|            todo = newtodo
#MANDO|UNUSED|        
#MANDO|UNUSED|        records = filter_records('authority', list)

#    def get_status(self, input_filter, output_fields):
#
#        # We should first check we can effectively use the credential
#
#        if 'slice_hrn' in input_filter:
#            slice_hrn = input_filter['slice_hrn']
#        slice_urn = hrn_to_urn(slice_hrn, 'slice')
#
##        slice_cred = self.get_slice_cred(slice_hrn).save_to_string(save_parents=True)
##        creds = [slice_cred]
##        if opts.delegate:
##            delegated_cred = self.delegate_cred(slice_cred, get_authority(self.authority))
##            creds.append(delegated_cred)
##        server = self.get_server_from_opts(opts)
##        return server.SliverStatus(slice_urn, creds)
#
#        try:
#            slice_cred = self.get_slice_cred(slice_hrn).save_to_string(save_parents=True)
#            creds = [slice_cred]
#        except:
#            # Fails if no right on slice, should use delegated credential
#            #delegated_cred = self.delegate_cred(slice_cred, get_authority(self.authority)) # XXX
#            #dest_fn = os.path.join(self.config['sfi_dir'], get_leaf(self.user) + "_slice_" + get_leaf(slice_hrn) + ".cred")
#            #str = file(dest_fn, "r").read()
#            #delegated_cred = str #Credential(string=str).save_to_string(save_parents=True)
#            #creds.append(delegated_cred) # XXX
#            cds = MySliceCredentials(self.api, {'credential_person_id': self.config['caller']['person_id'], 'credential_target': slice_hrn}, ['credential']) # XXX type
#            if not cds:
#                raise Exception, 'No credential available'
#            creds = [cds[0]['credential']]
#
#        #server = self.get_server_from_opts(opts)
#        ## direct connection to an aggregate
#        #if hasattr(opts, 'aggregate') and opts.aggregate:
#        #    server = self.get_server(opts.aggregate, opts.port, self.key_file, self.cert_file)
#        ## direct connection to the nodes component manager interface
#        #if hasattr(opts, 'component') and opts.component:
#        #    server = self.get_component_server_from_hrn(opts.component)
#
#        return self.sliceapi.SliverStatus(slice_urn, creds)

    @defer.inlineCallbacks
    def get_lease(self, user, user_config, filters, params, fields):
        result = yield self.get_resource_lease(user, user_config, filters,fields,params)
        defer.returnValue(result['lease'])

    @defer.inlineCallbacks
    def get_resource(self, user, user_config, filters, params, fields):
        result = yield self.get_resource_lease(user, user_config, filters, fields, params)
        defer.returnValue(result['resource'])

    @defer.inlineCallbacks
    def get_resource_lease(self, user, user_config, filters, params, fields):
        if self.user.email in DEMO_HOOKS:
            rspec = open('/usr/share/manifold/scripts/nitos.rspec', 'r')
            defer.returnValue(SFAGateway.parse_sfa_rspec(rspec))
            return 


        # Do we have a way to find slices, for now we only support explicit slice names
        # Note that we will have to inject the slice name into the resource object if not done by the parsing.
        # slice - resource is a NxN relationship, not well managed so far

        slice_hrns = make_list(filters.get_op('slice', (eq, included)))
        # XXX ONLY ONE AND WITHOUT JOKERS
        slice_hrn = slice_hrns[0] if slice_hrns else None

        # no need to check if server accepts the options argument since the options has
        # been a required argument since v1 API
        api_options = {}
        # always send call_id to v2 servers
        api_options ['call_id'] = unique_call_id()
        # ask for cached value if available
        api_options ['cached'] = True
        # Get server capabilities
        server_version = yield self.get_cached_server_version(self.sliceapi)
        type_version = set()

        # Versions matching to Gateway capabilities
        # We are implementing a negociation here:
        #  - remotely supported RSpec versions: geni_ad_rspec_versions
        #  - locally supported (SFAv1, GENIv3)
        # We build a list of supported tuples (type, version), and search a RSpec model in the intersection
        v = server_version['geni_ad_rspec_versions']
        for w in v:
            x = (w['type'], w['version'])
            type_version.add(x)
        local_version  = set([('SFA', '1'), ('GENI', '3')])
        common_version = type_version & local_version

        # TODO: Handle unkown verison of RSpec
        # We are using SFAv1 by default otherwise
        if not common_version:
            common_version.add(('SFA','1'))

        if ('SFA', '1') in common_version:
            api_options['geni_rspec_version'] = {'type': 'SFA', 'version': '1'}
        else:
            first_common = list(common_version)[0]
            api_options['geni_rspec_version'] = {'type': first_common[0], 'version': first_common[1]}

        if slice_hrn:
            cred = self._get_cred(user, user_config, 'slice', slice_hrn)
            api_options['geni_slice_urn'] = hrn_to_urn(slice_hrn, 'slice')
        else:
            cred = self._get_cred(user, user_config, 'user')

        if self.version['geni_api'] == 2:
            # AM API v2 
            result = yield self.sliceapi.ListResources([cred], api_options)
        else:
            # AM API v3
            if slice_hrn:
               slice_urn = api_options['geni_slice_urn']
               result = yield self.sliceapi.Describe([slice_urn], [cred], api_options)
               # dirty work around
               result['value'] = result['value']['geni_rspec']
            else:
               result = yield self.sliceapi.ListResources([cred], api_options)

        if not 'value' in result or not result['value']:
            raise Exception, result['output']

        rspec      = result['value']
        rsrc_slice = SFAGateway.parse_sfa_rspec(rspec)

        if slice_hrn:
            for r in rsrc_slice['resource']:
                r['slice'] = slice_hrn

        if self.debug:
            rsrc_slice['debug'] = {'rspec': rspec}
        defer.returnValue(rsrc_slice)

    def add_rspec_to_cache(self, slice_hrn, rspec):
        print "W: RSpec caching disabled"
        return
        # Cache result (XXX bug CreateSliver / need to invalidate former cache entries ?)
        # We might need to update a cached entry when modified instead of creating a new one
        rspec_add = {
            'rspec_person_id': self.config['caller']['person_id'],
            'rspec_target': slice_hrn,
            'rspec_hash': hashlib.md5(rspec).hexdigest(),
            #'rspec_expiration': XXX
            'rspec': rspec
        }
        new = MySliceRSpec(self.api, rspec_add)
        new.sync()
        if not new['rspec_id'] > 0:
            # WARNING: caching failed
            pass

################################################################################
# END SFA CODE
################################################################################

    def __init__(self, interface, platform, config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            config: A dictionnary containing the configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        super(SFAGateway, self).__init__(interface, platform, config)
#MANDO|
#MANDO|    def __init__(self, router, platform, query, config, user_config, user):
#MANDO|#        FromNode.__init__(self, platform, query, config)
#MANDO|        super(SFAGateway, self).__init__(router, platform, query, config, user_config, user)
        
        # self.config has always ['caller']
        # Check the presence of mandatory fields, default others
        #if not 'hashrequest' in self.config:    
        #    self.config['hashrequest'] = False
        #if not 'protocol' in self.config:
        #    self.config['protocol'] = 'xmlrpc'
        if not 'verbose' in self.config:
            self.config['verbose'] = 0
        if not 'sm' in self.config:
            raise Exception, "Missing SFA::sm parameter in configuration."
        if not 'debug' in self.config:
            self.config['debug'] = False
        if not 'registry' in self.config:
            raise Exception, "Missing SFA::registry parameter in configuration."
        # @loic Added default 5sec timeout if parameter self.config['timeout'] is not set
        if not 'timeout' in self.config:
            self.config['timeout'] = DEFAULT_TIMEOUT

    def __str__(self):
        return "<SFAGateway %r>" % (self.config['sm'])

    # MANDO: Adapted From Gateway
    @staticmethod
    def get_variables(user, user_config):
        assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = {}
        # Authenticated user
        variables['user_email'] = user.email
        for k, v in user.get_config().items():
            if isinstance(v, StringTypes) and not 'credential' in v:
                variables[k] = v
        # Account information of the authenticated user
        for k, v in user_config.items():
            if isinstance(v, StringTypes) and not 'credential' in v:
                variables[k] = v
        return variables

    # MANDO: Adapted From Gateway
    @staticmethod
    def start(user, user_config, query):
        assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        try:
            # Replaces variables in the Query (predicate in filters and parameters)
            filter = query.get_where()
            params = query.get_params()
            variables = SFAGateway.get_variables(user, user_config)

            for predicate in filter:
                value = predicate.get_value()

                # XXX variable support not implemented for lists and tuples
                if isinstance(value, (tuple, list)):
                    continue

                if value[0] == '$':
                    var = value[1:]
                    if var in variables:
                        predicate.set_value(variables[var])

            for key, value in params.items():

                # XXX variable support not implemented for lists and tuples
                if isinstance(value, (tuple, list)):
                    continue

                if value[0] == '$':
                    var = value[1:]
                    if var in variables and isinstance(variables[var], StringTypes):
                        params[k] = variables[var]
        except Exception, e:
            import traceback
            Log.warning("Exception in start", e)
            traceback.print_exc()

    def init_registry(self, admin_config):
        timeout = self.config['timeout']
        self.registry = make_sfa_proxy(self.config['registry'], admin_config, "gid", timeout)
        registry_hrn  = self.get_interface_hrn(self.registry)
        self.registry.set_network_hrn(registry_hrn)

    def init_agregate_manager(self, admin_config):
        timeout = self.config['timeout']
        self.sliceapi = make_sfa_proxy(self.config['sm'], admin_config, "gid", timeout)
        sm_hrn = self.get_interface_hrn(self.sliceapi)
        self.sliceapi.set_network_hrn(sm_hrn)

    @defer.inlineCallbacks
    def forward(self, query, callback, is_deferred = False, execute = True, user = None, format = "dict", receiver = None):
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
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        super(SFAGateway, self).forward(query, callback, is_deferred, execute, user, format, receiver)
        import traceback
        identifier = receiver.get_identifier() if receiver else None

        user_config = json.loads(user.config)
        SFAGateway.start(user, user_config, query)
        user = user.__dict__

        try:
            assert query, "Cannot run gateway with not query associated: %s" % self.platform
            self.debug = 'debug' in query.params and query.params['debug']

            # << bootstrap
            # Cache admin config
            admin_config = yield self.get_user_config(ADMIN_USER)
            assert admin_config, "Could not retrieve admin config"

            # Overwrite user config (reference & managed acccounts)
            new_user_config = yield self.get_user_config(user["email"])
            if new_user_config:
                user_config = new_user_config

            # Initialize manager proxies using MySlice Admin account
            try:
                self.init_registry(admin_config)
                self.init_agregate_manager(admin_config)
            except Exception, e:
                print "EXC in boostrap", e
                traceback.print_exc()
                self.error(receiver, query, str(e))
                return
            # >> bootstrap
 
            if not user_config:
                self.send(LAST_RECORD, callback, identifier)
                self.error(receiver, query, str(e))
                return
# <<
#MANDO|            fields = query.fields # Metadata.expand_output_fields(query.object, list(query.fields))
#MANDO|            result = yield getattr(self, "%s_%s" % (query.action, query.object))(query.filters, query.params, fields)
            Log.tmp("calling %s_%s" % (query.get_action(), query.get_from()))
            result = yield getattr(self, "%s_%s" % (query.get_action(), query.get_from()))(
                user,
                user_config,
                query.get_where(),
                query.get_params(),
                query.get_select()
            )
# >> 

            if query.get_from() in self.map_fields:
                Rename(receiver, self.map_fields[query.get_from()])
            
            for row in result:
                self.send(row, callback, identifier)
            self.success(receiver, query)

        except Exception, e:
            traceback.print_exc()
            self.error(receiver, query, str(e))

        self.send(LAST_RECORD, callback, identifier)

    @staticmethod
    def generate_slice_credential(target, config):
        Log.debug("Not implemented. Run delegation script in the meantime")
        #raise Exception, "Not implemented. Run delegation script in the meantime"
    
    # @loic delegate function is used to delegate a user credential to the ADMIN_USER
    def delegate(self, user_credential, user_private_key, user_gid, admin_credential):

       # if nessecary converting string to Credential object
        print "type of user cred = ",type(user_credential)
        if not isinstance (user_credential, Credential):
            print "this is not an object"
            user_credential = Credential (string=user_credential)
        print "type of user cred = ",type(user_credential)
        # How to set_passphrase of the PEM key if we don't have the  user password?
        # For the moment we will use PEM keys without passphrase

        # does the user has the right to delegate all its privileges?
        if not user_credential.get_privileges().get_all_delegate():
            raise Exception, "E: SFA Gateway the user has no right to delegate"

        # if nessecary converting string to Credential object
        if not isinstance (admin_credential, Credential):
            print "this is not an object"
            admin_credential = Credential (string=admin_credential)
        # get the admin_gid and admin_hrn from the credential
        print "admin get_gid_object()"
        admin_gid = admin_credential.get_gid_object()
        print "admin get_hrn()"
        admin_hrn = admin_gid.get_hrn()

        # Create temporary files for key and certificate in order to use existing code based on httplib 
        pkey_fn = tempfile.NamedTemporaryFile(delete=False) 
        pkey_fn.write(user_private_key.encode('latin1')) 
        cert_fn = tempfile.NamedTemporaryFile(delete=False) 
        cert_fn.write(user_gid) # We always use the GID 
        pkey_fn.close() 
        cert_fn.close() 
        print "cert=",cert_fn.name
        print "user_credential.delegate()",pkey_fn.name
        delegated_credential = user_credential.delegate(admin_gid, pkey_fn.name, cert_fn.name)
        print "delegated_cred to str"
        delegated_credential_str=delegated_credential.save_to_string(save_parents=True)

        os.unlink(pkey_fn.name) 
        os.unlink(cert_fn.name)
        return delegated_credential_str

    # TEST = PRESENT and NOT EXPIRED
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

    def credential_expired(self, cred):
        # if the cred passed as argument is not an object
        if not isinstance (cred, Credential):
            # from a string to a credential object to check expiration
            cred = Credential(string=cred)

        # check expiration of credentials
        return cred.get_expiration() < datetime.now()
   
    ############################################################################ 
    # ACCOUNT MANAGEMENT
    ############################################################################ 
    # using defer to have an asynchronous results management in functions prefixed by yield
    @defer.inlineCallbacks
    def manage(self, user, platform, config):
        Log.debug("Managing %r account on %s..." % (user, platform))
        # The gateway should be able to perform user config management taks on
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

        from sfa.trust.certificate import Keypair
        from sfa.util.xrn import Xrn, get_authority
        import json
        
        # Check fields that are present and credentials that are not expired
        # we will deduce the needed fields

        # The administrator is used as a mediator at the moment and thus does
        # not require any (delegated) credential. This could be modified in the
        # future if we expect MySlice to perform some operations on the testbed
        # under its name
        # NOTE We might want to manage a user account for direct use without
        # relying on delegated credentials. In this case we won't even have a
        # admin_config, and won't need delegation.
        is_admin = SFAGateway.is_admin(user)

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
        need_delegated_slice_credentials = not is_admin and self.credentials_needed('delegated_slice_credentials', config)
        need_delegated_authority_credentials = not is_admin and self.credentials_needed('delegated_authority_credentials', config)
        need_slice_credentials = need_delegated_slice_credentials
        need_slice_list = need_slice_credentials
        need_authority_credentials = need_delegated_authority_credentials
        need_authority_list = need_authority_credentials
        need_delegated_user_credential = not is_admin and self.credentials_needed('delegated_user_credential', config)
        need_gid = not 'gid' in config
        need_user_credential = need_authority_credentials or need_slice_list or need_slice_credentials or need_delegated_user_credential or need_gid

#MANDO|        if SFAGateway.is_admin(self.user):
#MANDO|            need_delegated_user_credential      = false
#MANDO|            need_delegated_slice_credential     = false
#MANDO|            need_delegated_authority_credential = false
        if SFAGateway.is_admin(user):
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
        
        if not 'user_hrn' in config:
            print "E: hrn needed to manage authentication"
            # return using asynchronous defer
            defer.returnValue({})
            #return {}

        if not 'user_private_key' in config:
            print "I: SFA::manage: Generating user private key for user", user
            k = Keypair(create=True)
            config['user_public_key'] = k.get_pubkey_string()
            config['user_private_key'] = k.as_pem()
            new_key = True

        if not 'sscert' in config:
            print "I: Generating self-signed certificate for user", user
            x = config['user_private_key'].encode('latin1')
            keypair = Keypair(string=x)
            self_signed = Certificate(subject = config['user_hrn'])
            self_signed.set_pubkey(keypair)
            self_signed.set_issuer(keypair, subject=config['user_hrn'].encode('latin1'))
            self_signed.sign()
            config['sscert'] = self_signed.save_to_string()

        # create an SFA connexion to Registry, using user config
        timeout = self.config['timeout']
        registry_proxy = make_sfa_proxy(self.config['registry'], config, 'sscert', timeout)
        if need_user_credential and self.credentials_needed('user_credential', config):
            Log.debug("Requesting user credential for user %s" % user)
            try:
                config['user_credential'] = yield registry_proxy.GetSelfCredential (config['sscert'], config['user_hrn'], 'user')
            except:
                # some urns hrns may replace non hierarchy delimiters '.' with an '_' instead of escaping the '.'
                hrn = Xrn(config['user_hrn']).get_hrn().replace('\.', '_')
                try:
                    config['user_credential'] = yield registry_proxy.GetSelfCredential (config['sscert'], hrn, 'user')
                except Exception, e:
                    raise Exception, "SFA Gateway :: manage() could not retreive user from SFA Registry: %s"%e

        # SFA call Reslove to get the GID and the slice_list
        if need_gid or need_slice_list:
            Log.debug("Generating GID for user %s" % user)
            records = yield registry_proxy.Resolve(config['user_hrn'].encode('latin1'), config['user_credential'])
            if not records:
                raise RecordNotFound, "hrn %s (%s) unknown to registry %s"%(config['user_hrn'],'user',registry_url)
            records = [record for record in records if record['type']=='user']
            record = records[0]
            config['gid'] = record['gid']
            try:
                config['slice_list'] = record['reg-slices']
            except Exception, e:
                Log.warning("User %s has no slices" % str(config['user_hrn']))

        # delegate user_credential
        if need_delegated_user_credential:
            Log.debug("I: SFA delegate user cred %s" % config['user_hrn'])
            config['delegated_user_credential'] = self.delegate(config['user_credential'], config['user_private_key'], config['gid'], self.admin_config['user_credential'])

        if need_authority_list: #and not 'authority_list' in config:
            config['authority_list'] = [get_authority(config['user_hrn'])]
 
        # Get Authority credential for each authority of the authority_list
        if need_authority_credentials: #and not 'authority_credentials' in config:
            Log.debug("Generating authority credentials for each authority")
            config['authority_credentials'] = {}
            try:
                for authority_name in config['authority_list']:
                    credential_string = yield registry_proxy.GetCredential (config['user_credential'], authority_name.encode('latin1'), 'authority')
                    config['authority_credentials'][authority_name] = credential_string
            except: pass # No authority credential

        # XXX TODO Factorization of slice and authority operations
        # Get Slice credential for each slice of the slice_list 
        if need_slice_credentials: 
            Log.debug("Generating slice credentials for each slice of the user")
            config['slice_credentials'] = {}
            for slice_hrn in config['slice_list']:
                # credential_string is temp, not delegated 
                credential_string = yield registry_proxy.GetCredential (config['user_credential'], slice_hrn.encode('latin1'), 'slice') 
                config['slice_credentials'][slice_hrn] = credential_string 
 
        if need_delegated_authority_credentials:
            Log.debug("Delegating authority credentials")
            config['delegated_authority_credentials'] = {}           
            for auth_name,auth_cred in config['authority_credentials'].items():
                delegated_auth_cred = self.delegate(auth_cred, config['user_private_key'], config['gid'], self.admin_config['user_credential'])                   
                config['delegated_authority_credentials'][auth_name] = delegated_auth_cred

        if need_delegated_slice_credentials:
            Log.debug("Delegating slice credentials")
            config['delegated_slice_credentials'] = {}
            for slice_hrn,slice_cred in config['slice_credentials'].items():
                delegated_slice_cred = self.delegate(slice_cred, config['user_private_key'], config['gid'], self.admin_config['user_credential'])      
                config['delegated_slice_credentials'][slice_hrn] = delegated_slice_cred

        # return using asynchronous defer
        defer.returnValue(config)

def sfa_trust_credential_delegate(self, delegee_gidfile, caller_keyfile, caller_gidfile):
    """
    Return a delegated copy of this credential, delegated to the 
    specified gid's user.    
    """
    # get the gid of the object we are delegating
    object_gid = self.get_gid_object()
    object_hrn = object_gid.get_hrn()

    # the hrn of the user who will be delegated to
    # @loic corrected
    print "gid type = ",type(delegee_gidfile)
    print delegee_gidfile.__class__
    if not isinstance(delegee_gidfile,GID):
        delegee_gid = GID(filename=delegee_gidfile)
    else:
        delegee_gid = delegee_gidfile
    delegee_hrn = delegee_gid.get_hrn()

    #user_key = Keypair(filename=keyfile)
    #user_hrn = self.get_gid_caller().get_hrn()
    subject_string = "%s delegated to %s" % (object_hrn, delegee_hrn)
    dcred = Credential(subject=subject_string)
    dcred.set_gid_caller(delegee_gid)
    dcred.set_gid_object(object_gid)
    dcred.set_parent(self)
    dcred.set_expiration(self.get_expiration())
    dcred.set_privileges(self.get_privileges())
    dcred.get_privileges().delegate_all_privileges(True)
    #dcred.set_issuer_keys(keyfile, delegee_gidfile)
    dcred.set_issuer_keys(caller_keyfile, caller_gidfile)
    dcred.encode()
    dcred.sign()

    return dcred
Credential.delegate = sfa_trust_credential_delegate
