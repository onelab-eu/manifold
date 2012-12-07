import sys
import os, os.path
import tempfile
import datetime
from lxml import etree
from StringIO import StringIO
from types import StringTypes, ListType
import re
import itertools
import urllib
import BeautifulSoup
import hashlib
import zlib
import copy # DIRTY HACK SENSLAB

from tophat.core.ast import FromNode
from tophat.util.faults import *

from tophat.core.filter import *
#from tophat.core.metadata import Metadata
from tophat.gateways.sfa.rspecs.SFAv1 import SFAv1Parser as Parser

from sfa.trust.certificate import Keypair, Certificate
from sfa.trust.gid import GID
from sfa.trust.credential import Credential
# from sfa.trust.sfaticket import SfaTicket

from sfa.util.sfalogging import sfi_logger
from sfa.util.xrn import Xrn, get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from sfa.util.config import Config
from sfa.util.version import version_core
from sfa.util.cache import Cache

from sfa.storage.record import Record

from sfa.rspecs.rspec import RSpec
#from sfa.rspecs.rspec_converter import RSpecConverter
from sfa.rspecs.version_manager import VersionManager

#from sfa.client.sfaclientlib import SfaClientBootstrap
from sfa.client.client_helper import pg_users_arg, sfa_users_arg
from sfa.client.sfaserverproxy import SfaServerProxy, ServerException
from sfa.client.return_value import ReturnValue
from tophat.models import User, Account, Platform, db
import json
import signal

from tophat.conf import ADMIN_USER, DEMO_HOOKS
import traceback

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

################################################################################
# Helper functions
################################################################################

def filter_records(type, records):
    filtered_records = []
    for record in records:
        if (record['type'] == type) or (type == "all"):
            filtered_records.append(record)
    return filtered_records

def project_select_and_rename_fields(table, pkey, filters, fields, map_fields=None):
    filtered = []
    for row in table:
        # translate field names
        if map_fields:
            for k, v in map_fields.items():
                if k in row:
                    if '.' in v: # users.hrn
                        method, key = v.split('.')
                        if not method in row:
                            row[method] = []
                        for x in row[k]:
                            row[method].append({key: x})        
                    else:
                        row[v] = row[k]
                    del row[k]
        # apply input filters # XXX TODO sort limit offset
        if filters.match(row):
            # apply output_fields
            c = {}
            for k,v in row.items():
                # if no fields = keep everything
                if not fields or k in fields or k == pkey:
                    c[k] = v
            filtered.append(c)
    return filtered

################################################################################

class TimeOutException(Exception):
    pass

def timeout(signum, frame):
    raise TimeOutException, "Command ran for too long"

def get_network_name(hostname):
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(5)
    try:
        soup = BeautifulSoup.BeautifulSoup(urllib.urlopen("http://%s"%hostname))
        t = soup.title.string
        if ' |' in t:
            name = t[:t.rindex(' |')]
        else:
            name = t
    except Exception, why:
        print why
        name = None
    signal.alarm(0)
    return name

import uuid
def unique_call_id(): return uuid.uuid4().urn


class SFA(FromNode):

################################################################################
# BEGIN SFA CODE
################################################################################

    # researcher == person ?
    map_slice_fields = {
        'last_updated': 'slice_last_updated', # last_updated != last == checked,
        'geni_creator': 'slice_geni_creator',
        'node_ids': 'slice_node_ids',       # X This should be 'nodes.id' but we do not want IDs
        'researcher': 'users.person_hrn',   # This should be 'users.hrn'
        'site_id': 'slice_site_id',         # X ID 
        'site': 'slice_site',               # authority.hrn
        'authority': 'authority_hrn',       # isn't it the same ???
        'pointer': 'slice_pointer',         # X
        'instantiation': 'slice_instantiation',# instanciation
        'max_nodes': 'slice_max_nodes',     # max nodes
        'person_ids': 'slice_person_ids',   # X users.ids
        'hrn': 'slice_hrn',                 # hrn
        'record_id': 'slice_record_id',     # X
        'gid': 'slice_gid',                 # gid
        'nodes': 'nodes',                   # nodes.hrn
        'peer_id': 'slice_peer_id',         # X
        'type': 'slice_type',               # type ?
        'peer_authority': 'slice_peer_authority', # ??
        'description': 'slice_description', # description
        'expires': 'slice_expires',         # expires
        'persons': 'slice_persons',         # users.hrn
        'creator_person_id': 'slice_creator_person_id', # users.creator ?
        'PI': 'slice_pi',                   # users.pi ?
        'name': 'slice_name',               # hrn
        #'slice_id': 'slice_id',
        'created': 'created',               # first ?
        'url': 'slice_url',                 # url
        'peer_slice_id': 'slice_peer_slice_id', # ?
        'geni_urn': 'slice_geni_urn',       # urn/hrn
        'slice_tag_ids': 'slice_tag_ids',   # tags
        'date_created': 'slice_date_created'# first ?
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
        'slices': 'user_slices'                     # OBJ slices
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
    
    # init self-signed cert, user credentials and gid
    def bootstrap (self):

        # Get the account of the admin user in the database
        try:
            user = db.query(User).filter(User.email == ADMIN_USER).one()
        except Exception, e:
            raise Exception, 'Missing admin user: %s' % str(e)
            # No admin user account, let's create one
            #user = User(email=ADMIN_USER)
            #db.add(user)
            #db.commit()

        # Get platform
        platform = db.query(Platform).filter(Platform.platform == self.platform).one()

        # Get user account
        accounts = [a for a in user.accounts if a.platform == platform]
        if not accounts:
            raise Exception, "Accounts should be created for MySlice admin user"
            # Let's make sure 'ple' reference account exists
            #ref_accounts = [a for a in user.accounts if a.platform.platform == 'ple']
            #if not ref_accounts:
            #    ref_platform = db.query(Platform).filter(Platform.platform == 'ple').one()
            #    ref_config = {
            #        'user_hrn': 'ple.upmc.slicebrowser',
            #        'user_private_key': 'XXX' .encode('latin1')
            #    }
            #    ref_account = Account(user=user, platform=ref_platform, auth_type='managed', config=json.dumps(ref_config))
            #    db.add(ref_account)
            #
            #if platform.platform != 'ple':
            #    account = Account(user=user, platform=platform, auth_type='reference', config='{"reference_platform": "ple"}')
            #    db.add(account)
            #db.commit()
        else:
            account = accounts[0]

        config_new = None
        if account.auth_type == 'reference':
            ref_platform = json.loads(account.config)['reference_platform']
            ref_platform = db.query(Platform).filter(Platform.platform == ref_platform).one()
            ref_accounts = [a for a in user.accounts if a.platform == ref_platform]
            if not ref_accounts:
                raise Exception, "reference account does not exist"
            ref_account = ref_accounts[0]
            config_new = json.dumps(SFA.manage(ADMIN_USER, ref_platform, json.loads(ref_account.config)))
            if ref_account.config != config_new:
                ref_account.config = config_new
                db.add(ref_account)
                db.commit()
        else:
            config_new = json.dumps(SFA.manage(ADMIN_USER, platform, json.loads(account.config)))
            if account.config != config_new:
                account.config = config_new
                db.add(account)
                db.commit()

        # Initialize manager proxies

        reg_url = self.config['registry']
        sm_url = self.config['sm']
        if not sm_url.startswith('http://') or sm_url.startswith('https://'):
            sm_url = 'http://' + sm_url

        config = json.loads(config_new)
        pkey_fn = tempfile.NamedTemporaryFile(delete=False)
        pkey_fn.write(config['user_private_key'].encode('latin1'))
        cert_fn = tempfile.NamedTemporaryFile(delete=False)
        cert_fn.write(config['gid']) 
        pkey_fn.close()
        cert_fn.close()

        self.registry = SfaServerProxy(reg_url, pkey_fn.name, cert_fn.name,
                timeout=self.config['timeout'],
                verbose=self.config['debug'])  
        self.sliceapi = SfaServerProxy(sm_url, pkey_fn.name, cert_fn.name,
                timeout=self.config['timeout'],
                verbose=self.config['debug'])  

        print "leaves temp files"
        #os.unlink(pkey_fn.name)
        #os.unlink(cert_fn.name)


    def get_cached_server_version(self, server):
        # check local cache first
        version = None 
        cache_key = server.url + "-version"
        cache = Cache()

        if cache:
            version = cache.get(cache_key)

        if not version: 
            result = server.GetVersion()
            version= ReturnValue.get_value(result)
            # cache version for 20 minutes
            cache.add(cache_key, version, ttl= 60*20)
            self.logger.info("Updating cache")

        return version   
        
    ### resurrect this temporarily so we can support V1 aggregates for a while
    def server_supports_options_arg(self, server):
        """
        Returns true if server support the optional call_id arg, false otherwise. 
        """
        server_version = self.get_cached_server_version(server)
        result = False
        # xxx need to rewrite this 
        # XXX added not server version to handle cases where GetVersion fails (jordan)
        if not server_version or int(server_version.get('geni_api')) >= 2:
            result = True
        return result

    def server_supports_call_id_arg(self, server):
        server_version = self.get_cached_server_version(server)
        result = False      
        if 'sfa' in server_version and 'code_tag' in server_version:
            code_tag = server_version['code_tag']
            code_tag_parts = code_tag.split("-")
            version_parts = code_tag_parts[0].split(".")
            major, minor = version_parts[0], version_parts[1]
            rev = code_tag_parts[1]
            if int(major) == 1 and minor == 0 and build >= 22:
                result = True
        return result                 

    ### ois = options if supported
    # to be used in something like serverproxy.Method (arg1, arg2, *self.ois(api_options))
    def ois (self, server, option_dict):
        if self.server_supports_options_arg (server): 
            return [option_dict]
        elif self.server_supports_call_id_arg (server):
            return [ unique_call_id () ]
        else: 
            return []

    ### cis = call_id if supported - like ois
    def cis (self, server):
        if self.server_supports_call_id_arg (server):
            return [ unique_call_id ]
        else:
            return []

    ############################################################################ 
    #
    # SFA Method wrappers
    #
    ############################################################################ 

    def sfa_get_slices_hrn(self, cred):
        api_options = {}
        api_options['call_id']=unique_call_id()
        results = self.sliceapi.ListSlices(cred, *self.ois(self.sliceapi,api_options)) # user cred
        results = results['value']
        #{'output': '', 'geni_api': 2, 'code': {'am_type': 'sfa', 'geni_code': 0, 'am_code': None}, 'value': [
        return [urn_to_hrn(r)[0] for r in results]

    def sfa_list_records(self, cred, hrns, record_type=None):
        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
            raise PLCInvalidArgument('Wrong filter in sfa_list')
        records = self.registry.List(hrns, cred)
        if record_type:
            records = filter_records(record_type, records)
        return records

    def sfa_resolve_records(self, cred, xrns, record_type=None):
        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
            raise PLCInvalidArgument('Wrong filter in sfa_list')

        #try:
        print "CONNECTING TO REGISTRY", self.registry
        records = self.registry.Resolve(xrns, cred, {'details': True})
        #except Exception, why:
        #    print "[Sfa::sfa_resolve_records] ERROR : %s" % why
        #    return []

        if record_type:
            records = filter_records(record_type, records)

        return records

    def sfa_get_resources(self, cred, hrn=None):
        # no need to check if server accepts the options argument since the options has
        # been a required argument since v1 API
        api_options = {}
        # always send call_id to v2 servers
        api_options ['call_id'] = unique_call_id()
        # ask for cached value if available
        api_options ['cached'] = True
        #api_options['info'] = options.info
        #if options.rspec_version:
        #    version_manager = VersionManager()
        #    server_version = self.get_cached_server_version(server)
        #    if 'sfa' in server_version:
        #        # just request the version the client wants
        #        api_options['geni_rspec_version'] = version_manager.get_version(options.rspec_version).to_dict()
        #    else:
        #        api_options['geni_rspec_version'] = {'type': 'geni', 'version': '3.0'}
        #else:
        # {'output': ': ListResources: Unsupported RSpec version: [geni 3.0 None] is not suported here', 'geni_api': 2, 'code': {'am_type': 'sfa', 'geni_code': 13, 'am_code': 13}, 'value': ''}
        #api_options['geni_rspec_version'] = {'type': 'geni', 'version': '3.0'}
        api_options['geni_rspec_version'] = {'type': 'SFA', 'version': '1'}
        #api_options['geni_rspec_version'] = {'type': 'ProtoGENI', 'version': '2'}
        #api_options['geni_rspec_version'] = {'type': 'GENI', 'version': '3'}

        if hrn:
            api_options['geni_slice_urn'] = hrn_to_urn(hrn, 'slice')
        result = self.sliceapi.ListResources(cred, api_options)
        return ReturnValue.get_value(result)

    ############################################################################ 
    #
    # GETVERSION & RECURSIVE SCAN
    #
    ############################################################################ 

    # All commands should take a registry/sliceapi as a parameter to allow for
    # more than one

    # type IN (aggregate, registry, local)
    def sfa_get_version(self, type='aggregate', url=None):
        if url:
            return {}

        if type == 'local':
            version=version_core()
        else:
            if type == 'registry':
                server = self.registry
            else:
                server = self.sliceapi
            result = server.GetVersion()
            version = ReturnValue.get_value(result)
        return version


    # scan from the given interfacs as entry points
    # XXX we should be able to run this in parallel
    # which ones are seen but do not reply
    # how to group emulab
    def sfa_get_version_rec(self,interfaces):
        output = []
        if not isinstance(interfaces,list):
            interfaces=[interfaces]

        # add entry points right away using the interface uid's as a key
        to_scan=interfaces
        scanned=[]
        # keep on looping until we reach a fixed point
        while to_scan:
            for interface in to_scan:

                # performing xmlrpc call
                print "D: Connecting to interface", interface
                server = SfaServerProxy(interface, self.private_key, self.my_gid, timeout=5)
                try:
                    version = ReturnValue.get_value(server.GetVersion())
                except Exception, why:
                    print "E: ", why
                    version = None

                if version:
                    output.append(version)
                    if 'peers' in version: 
                        for (next_name,next_url) in version['peers'].iteritems():
                            if not next_url in scanned:
                                to_scan.append(next_url)
                scanned.append(interface)
                to_scan.remove(interface)
        return output

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

    def parse_sfa_rspec(self, rspec):
        parser = Parser(rspec)
        return parser.to_dict()

    def build_sfa_rspec(self, slice_id, resources, leases):
        parser = Parser(resources, leases)
        return parser.to_rspec(slice_id)


    ############################################################################ 
    #
    # COMMANDS
    #
    ############################################################################ 


    # get a delegated credential of a given type to a specific target
    # default allows the use of MySlice's own credentials
    def _get_cred(self, type, target=None):
        if type == 'user':
            if target:
                raise Exception, "Cannot retrieve specific user credential for now"
            return self.user_config['user_credential']
        elif type == 'authority':
            if target:
                raise Exception, "Cannot retrieve specific authority credential for now"
            return self.user_config['authority_credential']
        elif type == 'slice':
            if not 'slice_credentials' in self.user_config:
                self.user_config['slice_credentials'] = {}

            creds = self.user_config['slice_credentials']
            if target in creds:
                cred = creds[target]
            else:
                # Can we generate them : only if we have the user private key
                # Currently it is not possible to request for a slice credential
                # with a delegated user credential...
                if 'user_private_key' in self.user_config and self.user_config['user_private_key']:
                    cred = SFA.generate_slice_credential(target, self.user_config)
                    creds[target] = cred
                else:
                    raise Exception , "no cred found of type %s towards %s " % (type, target)
            return cred
        else:
            raise Exception, "Invalid credential type: %s" % type

    def update_slice(self, filters, params, fields):
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
        user_cred = self._get_cred('user')
        slice_cred = self._get_cred('slice', slice_hrn)

        # We suppose resource
        rspec = self.build_sfa_rspec(slice_urn, resources, leases)
        print "BUILDING SFA RSPEC", rspec

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
        slice_records = self.registry.Resolve(slice_urn, [user_cred])
        
        # Due to a bug in the SFA implementation when Resolve requests are
        # forwarded, records are not filtered (Resolve received a list of xrns,
        # does not resolve its type, then issue queries to the local database
        # with the hrn only)
        print "W: SFAWrap bug workaround"
        slice_records = Filter.from_dict({'type': 'slice'}).filter(slice_records)

        # slice_records = self.registry.Resolve(slice_urn, [self.my_credential_string], {'details':True})
        if slice_records and 'reg-researchers' in slice_records[0] and slice_records[0]['reg-researchers']:
            slice_record = slice_records[0]
            user_hrns = slice_record['reg-researchers']
            user_urns = [hrn_to_urn(hrn, 'user') for hrn in user_hrns]
            user_records = self.registry.Resolve(user_urns, [user_cred])
            server_version = self.get_cached_server_version(self.registry)
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
        result = self.sliceapi.CreateSliver(slice_urn, [slice_cred], rspec, users, *self.ois(self.sliceapi, api_options))
        print "CreateSliver RSPEC"
        manifest = ReturnValue.get_value(result)
        print "MANIFEST: ", str(result)[:100]

        if not manifest:
            print "NO MANIFEST"
            return []
        rsrc_leases = self.parse_sfa_rspec(manifest)

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        print "oK"
        return [slice]

    # minimally check a key argument
    def check_ssh_key(self, key):
        good_ssh_key = r'^.*(?:ssh-dss|ssh-rsa)[ ]+[A-Za-z0-9+/=]+(?: .*)?$'
        return re.match(good_ssh_key, key, re.IGNORECASE)

    def create_record_from_params(self, type, params):
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
            if not self.check_ssh_key(pubkey):
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
 
    def create_slice(self, filters, params, fields):

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
        cred = self._get_cred('authority')
        record_dict = self.create_record_from_params('slice', params)
        try:
            slice_gid = self.registry.Register(record_dict, cred)
        except Exception, e:
            # sfa.client.sfaserverproxy.ServerException: : Register: Existing record: ple.upmc.myslicedemo2, 
            print "E: %s" % e
        return []

    def _get_slices_hrn(self, filters = None):
        #    Depending on the input_filters, we can use a more or less
        #    extended query that will limit[cred] filtering a posteriori
        slice_list = []

        cred = None
        if not filters or not filters.has_eq('slice_hrn') or filters.has_eq('authority_hrn'):
        #if not input_filter or 'slice_hrn' not in input_filter or 'authority_hrn' in input_filter:
            #cred = [self._get_cred('user', self.config['caller']['person_hrn'],default=True)]
            cred = self._get_cred('user')

        if not filters or not (filters.has_eq('slice_hrn') or filters.has_eq('authority_hrn') or filters.has_op('users.person_hrn', contains)):
        #if not input_filter or not ('slice_hrn' in input_filter or 'authority_hrn' in input_filter or '{users.person_hrn' in input_filter):
            # no details specified, get the full list of slices
            return self.sfa_get_slices_hrn(cred)

        # XXX We would need the subquery for this !!
        if filters.has_op('users.person_hrn', contains):
            hrn = filters.get_op('users.person_hrn', contains)
            auth = hrn[:hrn.rindex('.')]
            records = self.sfa_list_records(cred, auth, 'slice')
            slice_list = [r['hrn'] for r in records]
        else:
            if filters.has_eq('authority_hrn'): # XXX recursive modifiers ?
                # Get the list of slices
                # record fields: peer_authority, last_updated, type, authority, hrn, gid, record_id, date_created, pointer
                auths = filters.get_eq('authority_hrn')
                if not isinstance(auths, list):
                    auths = [auths]

                for hrn in auths:
                    records = self.sfa_list_records(cred, hrn, 'slice')
                    slice_list.extend([r['hrn'] for r in records])

            if filters.has_eq('slice_hrn'):
                hrns = filters.get_eq('slice_hrn')
                if not isinstance(hrns, list):
                    hrns = [hrns]
                slice_list.extend(hrns)

        return slice_list        

    def get_user(self, filters = None, params = None, fields = None):
        pass 


    def get_slice(self, filters = None, params = None, fields = None):

        #
        # DEMO hook
        #
        if self.user.email in DEMO_HOOKS:
            print "W: Demo hook"
            s= {}
            s['slice_hrn'] = "ple.upmc.agent"
            s['slice_description'] = 'DEMO SLICE'

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
                rsrc_leases = self.get_resource_lease({'slice_hrn': 'ple.upmc.agent'}, subfields)
                if not rsrc_leases:
                    raise Exception, 'get_resources failed!'
                s['resource'] = rsrc_leases['resource']
                s['lease'] = rsrc_leases['lease'] 
            if has_users:
                s['users'] = [{'person_hrn': 'myslice.demo'}]
            if self.debug:
                s['debug'] = rsrc_leases['debug']

            return [s]
        #
        # END: DEMO
        #

        # A/ List slices hrn XXX operator on slice_hrn
        slice_list = self._get_slices_hrn(filters)
        if slice_list == []:
            return slice_list
        

        # B/ Get slice information
        
        # - Do we have a need for filtering or additional information ?
        if fields == set(['slice_hrn']) and not filters: # XXX we should also support slice_urn etc.
            # No need for filtering or additional information
            return [{'slice_hrn': hrn} for hrn in slice_list]
            
        # - Do we need to filter the slice_list by authority ?
        if filters.has('authority_hrn'):
            predicates = filters.get_predicates('authority_hrn')
            for p in predicates:
                slice_list = [s for s in slice_list if p.match({'authority_hrn': get_authority(s)})]
        if slice_list == []:
            return slice_list

        # Depending on the information we need, we don't need to call Resolve
        # since for a large list of slices will take some time
        # only slices that belong to my site !!!!
        # XXX better: we only ask slices from the institution
        #if '{users.person_hrn' in input_filter:
        #    # We can restrict slices to slices from the same institution
        #    hrn = input_filter['{users.person_hrn']
        #    try:
        #        auth = hrn[:hrn.rindex('.')+1]
        #        slice_list = filter(lambda x: x.startswith(auth), slice_list)
        #    except ValueError:
        #        pass

        # We need user credential here (already done before maybe XXX)
        # cred = self._get_cred('user', self.user.user_hrnself.config['caller']['person_hrn'])
        cred = self._get_cred('user')
        # - Resolving slice hrns to get additional information
        slices = self.sfa_resolve_records(cred, slice_list, 'slice')
        # Merge resulting information
        for (hrn, slice) in itertools.izip(slice_list, slices):
            slice['slice_hrn'] = hrn

        # Selection

        # Projection and renaming
        #print "SLICES BEFORE FILTERING:", slices
        #print "USERS OF SLICE[0] =", slices[0]['reg-researchers']
        filtered = project_select_and_rename_fields(slices, 'slice_hrn', filters, fields, self.map_slice_fields)
        # XXX generic function to manage subrequests
        
        # Manage subqueries
        has_resource = False
        has_lease = False
        has_user = False
        for of in fields:
            if of == 'resource' or of.startswith('resource.'):
                has_resource = True
            if of == 'lease' or of.startswith('lease.'):
                has_lease = True
            if of == 'user' or of.startswith('user.'):
                has_user = True

        if has_resource or has_lease:
            # = what we have in RSpecs
            # network, site, node, hostname, slice tags, sliver tags, nodes in slice and not in slice
            # We might not need Resolve if we have all necessary information here
            for s in filtered:
                # we loop since each slice requires a different credential
                # XXX  how to tell the user we miss some credentials
                hrn = s['slice_hrn']
                subfields = []
                if has_resource: subfields.append('resource')
                if has_lease: subfields.append('lease')
                rsrc_leases = self.get_resource_lease({'slice_hrn': hrn}, subfields)
                if not rsrc_leases:
                    print "W: Could not collect resource/leases for slice %s" % hrn
                if has_resource:
                    s['resource'] = rsrc_leases['resource']
                if has_lease:
                    s['lease'] = rsrc_leases['lease'] 
                if self.debug:
                    s['debug'] = rsrc_leases['debug']

        if has_user:
            pass # TODO how to get slice users

        # remove join fields
        if 'slice_hrn' not in fields:
            for s in filtered:
                del s['slice_hrn']

        return filtered

    def get_user(self, filters = None, params = None, fields = None):

        cred = self._get_cred('user')

        # A/ List users
        if not filters or not (filters.has_eq('user_hrn') or filters.has_eq('authority_hrn')):
            # no authority specified, we get all users *recursively*
            raise Exception, "E: Recursive user listing not implemented yet."

        elif filters.has_eq('authority_hrn'):
            # Get the list of users
            auths = filters.get_eq('authority_hrn')
            if not isinstance(auths, list): auths = [auths]

            # Get the list of user_hrn
            user_list = []
            for hrn in auths:
                ul = self.registry.List(hrn, cred)
                ul = filter_records('user', ul)
                user_list.extend([r['hrn'] for r in ul])

        else: # named users
            user_list = filters.get_eq('user_hrn')
            if not isinstance(user_list, list): user_list = [user_list]
        
        if not user_list: return user_list

        # B/ Get user information
        if filters == set(['user_hrn']): # urn ?
            return [ {'user_hrn': hrn} for hrn in user_list ]

        else:
            # Here we could filter by authority if possible
            if filters.has_eq('authority_hrn'):
                predicates = filters.get_predicates('authority_hrn')
                for p in predicates:
                    user_list = [s for s in user_list if p.match({'authority_hrn': get_authority(s)})]

            if not user_list: return user_list

            print "resolve to registry", self.registry
            users = self.registry.Resolve(user_list, cred)
            users = filter_records('user', users)
            filtered = []

            for user in users:
                # translate field names...
                for k,v in self.map_user_fields.items():
                    if k in user:
                        user[v] = user[k]
                        del user[k]
                # apply input_filters XXX TODO sort limit offset
                if filters.match(user):
                    # apply output_fields
                    c = {}
                    for k,v in user.items():
                        if k in fields:
                            c[k] = v
                    filtered.append(c)

            return filtered


    def sfa_table_networks(self):
        versions = self.sfa_get_version_rec(self.sm_url)

        output = []
        for v in versions:
            hrn = v['hrn']
            networks = [x for x in output if x['network_hrn'] == hrn]
            if networks:
                print "I: %s exists!" % hrn
                continue

            # XXX we might make temporary patches for ppk for example
            if 'hostname' in v and v['hostname'] == 'ppkplc.kaist.ac.kr':
                print "[FIXME] Hardcoded hrn value for PPK"
                v['hrn'] = 'ppk'

            # We skip networks that do not advertise their hrn
            # XXX TODO issue warning
            if 'hrn' not in v:
                continue

            # Case when hostnames differ

            network = {'network_hrn': v['hrn']}
            # Network name
            #name = None
            #if 'hostname' in version:
            #    name = get_network_name(version['hostname'])
            #if not name:
            #    name = get_network_name(i.hostname)
            #if name:
            #    network['network_name'] = name
            output.append(network)

        # same with registries
        return output

    def get_networks(self, input_filter = None, output_fields = None):
        networks = self.sfa_table_networks()
        return project_select_and_rename_fields(networks, 'network_hrn', input_filter, output_fields)

    def get_recauth(self, input_filter = None, output_fields = None):
        user_cred = self.get_user_cred().save_to_string(save_parents=True)
        records = self.get_networks()
        todo = [r['network_hrn'] for r in records]
        while todo:
            newtodo = []
            for hrn in todo:
                try:
                    records = self.registry.List(hrn, user_cred)
                except Exception, why:
                    print "Exception during %s: %s" % (hrn, str(why))
                    continue
                records = filter_records('authority', records)
                newtodo.extend([r['hrn'] for r in records])
            todo = newtodo
        
        records = filter_records('authority', list)

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


    def get_resource(self, filters, params, fields):
        result = self.get_resource_lease(filters, fields, params)
        return result['resource']

    def get_resource_lease(self, input_filter = None, params = None, output_fields = None):
        # DEMO
        if self.user.email in DEMO_HOOKS:
            #rspec = open('/usr/share/myslice/scripts/sample-sliver.rspec', 'r')
            rspec = open('/usr/share/myslice/scripts/nitos.rspec', 'r')
            return self.parse_sfa_rspec(rspec)

            # Add random lat-lon values
            #import random
            #for r in resources:
            #    lat = random.random() * 180. - 90.
            #    lon = random.random() * 360. - 180.
            #    r['latitude'] = lat
            #    r['longitude'] = lon
        # END DEMO

        try:
            if input_filter and 'slice_hrn' in input_filter:
                hrn = input_filter['slice_hrn']
                cred = self._get_cred('slice', hrn)
            else:
                hrn = None
                #cred = self._get_cred('user', self.config['caller']['person_hrn'])
                cred = self._get_cred('user')
        
            # We request the list of nodes in the slice
            rspec = self.sfa_get_resources(cred, hrn)
            rsrc_slice = self.parse_sfa_rspec(rspec)

            # and the full list of nodes (XXX this could be cached)
            rspec = self.sfa_get_resources(cred)
            rsrc_all = self.parse_sfa_rspec(rspec)

            # List of nodes and leases present in the slice (check for 'sliver' is redundant)
            sliver_urns = [ r['urn'] for r in rsrc_slice['resource'] if 'sliver' in r ]
            lease_urns = [ l['urn'] for l in rsrc_slice['lease'] ]

            # We now build the final answer where the resources have all nodes...
            for r in rsrc_all['resource']:
                if not r['urn'] in sliver_urns:
                    rsrc_slice['resource'].append(r)

            # Adding leases for nodes not in the slice
            for l in rsrc_all['lease']:
                # Don't add if we already have it
                if Xrn(l['slice_id']).hrn != hrn:
                    rsrc_slice['lease'].append(l)

            # Adding fake lease for all reservable nodes that do not have leases already
            print "W: removed fake leases: TO TEST"
            #for r in rsrc_slice['resource']:
            #    if ('exclusive' in r and r['exclusive'] in ['TRUE', True] and not r['urn'] in lease_urns) or (r['type'] == 'channel'):
            #        #urn = r['urn']
            #        #xrn = Xrn(urn)
            #        fake_lease = {
            #            'urn': r['urn'],
            #            'hrn': r['hrn'],
            #            'type': r['type'],
            #            'network': r['network'], #xrn.authority[0],
            #            'start_time': 0,
            #            'duration': 0,
            #            'granularity': 0,
            #            'slice_id': None
            #        }
            #        rsrc_slice['lease'].append(fake_lease)
            if self.debug:
                rsrc_slice['debug'] = {'rspec': rspec}

            return rsrc_slice
            
        except Exception, e:
            print "E: get_resource", e
            ret = {'resource': [], 'lease': []}
            # EXCEPTIONS Some tests about giving back informations
            if self.debug:
                exc = {'context': 'get_resource_lease', 'e': e}
                ret['debug'] = {'exception': exc}
            return ret

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

    def __init__(self, router, platform, query, config, user_config, user):
#        FromNode.__init__(self, platform, query, config)
        super(SFA, self).__init__(router, platform, query, config, user_config, user)
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
        if not 'timeout' in self.config:
            self.config['timeout'] = None
        self.debug = 'debug' in query.params and query.params['debug']

        self.logger = sfi_logger

        # Check user accounts & prepare managers proxy
        #self.bootstrap()

    def __str__(self):
        return "<SFAGateway %r: %s>" % (self.config['sm'], self.query)

    def do_start(self):
        if not self.user_config:
            self.callback(None)
            return
        try:
            self.bootstrap()
            q = self.query
            # Let's call the simplest query as possible to begin with
            # This should use twisted XMLRPC

            # Hardcoding the get network call until caching is implemented
            if q.action == 'get' and q.fact_table == 'network':
                platforms = db.query(Platform).filter(Platform.disabled == False).all()
                output = []
                for p in platforms:
                    self.callback({'network_hrn': p.platform, 'network_name': p.platform_longname})
                self.callback(None)
                return

            # DIRTY HACK to allow slices to span on non federated testbeds
            #
            # user account will not reference another platform, and will implicitly
            # contain information about the slice to associate USERHRN_slice
            slice_hrn = None
            if self.platform == 'senslab' and q.fact_table == 'slice' and q.filters.has_eq('slice_hrn'):
                slice_hrn = q.filters.get_eq('slice_hrn')
                if not self.user_config or not 'user_hrn' in self.user_config:
                    raise Exception, "Missing user configuration"
                senslab_slice = '%s_slice' % self.user_config['user_hrn']
                print "I: Using slice %s for senslab platform" % senslab_slice
                local_filters = copy.deepcopy(q.filters)
                local_filters.set_eq('slice_hrn', senslab_slice)
            else:
                local_filters = q.filters
            
            fields = q.fields # Metadata.expand_output_fields(q.fact_table, list(q.fields))
            result = getattr(self, "%s_%s" % (q.action, q.fact_table))(local_filters, q.params, fields)
            for r in result:
                # DIRTY HACK continued
                if slice_hrn and 'slice_hrn' in r:
                    r['slice_hrn'] = slice_hrn
                self.callback(r)
        except Exception, e:
            print "W: Exception during SFA operation, ignoring...%s" % str(e)
            traceback.print_exc()


        self.callback(None)


    @staticmethod
    def generate_slice_credential(target, config):
        raise Exception, "Not implemented. Run delegation script in the meantime"
        

    @staticmethod
    def manage(user, platform, config):
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
        from sfa.util.xrn import Xrn
        import json

        new_key = False

        if not 'user_hrn' in config:
            print "E: hrn needed to manage authentication"
            return {}

        if not 'user_private_key' in config:
            print "I: SFA::manage: Generating user private key ofr user", user
            k = Keypair(create=True)
            config['user_public_key'] = k.get_pubkey_string()
            config['user_private_key'] = k.as_pem()
            new_key = True

        if new_key or not 'sscert' in config or not config['sscert']:
            print "I: Generating self-signed certificate for user", user
            x = config['user_private_key'].encode('latin1')
            keypair = Keypair(string=x)
            self_signed = Certificate(subject = config['user_hrn'])
            self_signed.set_pubkey(keypair)
            self_signed.set_issuer(keypair, subject=config['user_hrn'].encode('latin1'))
            self_signed.sign()
            config['sscert'] = self_signed.save_to_string()

        if new_key or not 'user_credential' in config: # or expired
            print "I: SFA::manage: Requesting user credential for user", user
            # Create temporary files for key and certificate in order to use existing code based on httplib
            pkey_fn = tempfile.NamedTemporaryFile(delete=False)
            pkey_fn.write(config['user_private_key'].encode('latin1'))
            cert_fn = tempfile.NamedTemporaryFile(delete=False)
            cert_fn.write(config['sscert'])
            pkey_fn.close()
            cert_fn.close()

            # We need to connect through a HTTPS connection using the generated private key
            registry_url = json.loads(platform.config)['registry']
            registry_proxy = SfaServerProxy (registry_url, pkey_fn.name, cert_fn.name)

            try:
                credential_string = registry_proxy.GetSelfCredential (config['sscert'], config['user_hrn'], 'user')
            except:
                # some urns hrns may replace non hierarchy delimiters '.' with an '_' instead of escaping the '.'
                hrn = Xrn(config['user_hrn']).get_hrn().replace('\.', '_')
                credential_string=registry_proxy.GetSelfCredential (config['sscert'], hrn, 'user')

            config['user_credential'] = credential_string

            os.unlink(pkey_fn.name)
            os.unlink(cert_fn.name)

        if new_key or not 'gid' in config:
            print "I: Generating GID for user", user
            # Create temporary files for key and certificate in order to use existing code based on httplib
            pkey_fn = tempfile.NamedTemporaryFile(delete=False)
            pkey_fn.write(config['user_private_key'].encode('latin1'))
            cert_fn = tempfile.NamedTemporaryFile(delete=False)
            cert_fn.write(config['sscert'])
            pkey_fn.close()
            cert_fn.close()

            # We need to connect through a HTTPS connection using the generated private key
            registry_url = json.loads(platform.config)['registry']
            registry_proxy = SfaServerProxy(registry_url, pkey_fn.name, cert_fn.name)

            records = registry_proxy.Resolve(config['user_hrn'].encode('latin1'), config['user_credential'])
            records = [record for record in records if record['type']=='user']
            if not records:
                raise RecordNotFound, "hrn %s (%s) unknown to registry %s"%(config['user_hrn'],'user',self.registry_url)
            record = records[0]
            config['gid'] = record['gid']

            os.unlink(pkey_fn.name)
            os.unlink(cert_fn.name)

        if new_key or not 'authority_credential' in config:
            print "I: Generating authority credential for user", user
            # Same code for slice credentials...

            # Create temporary files for key and certificate in order to use existing code based on httplib
            pkey_fn = tempfile.NamedTemporaryFile(delete=False)
            pkey_fn.write(config['user_private_key'].encode('latin1'))
            cert_fn = tempfile.NamedTemporaryFile(delete=False)
            cert_fn.write(config['gid']) # We always use the GID
            pkey_fn.close()
            cert_fn.close()

            # We need to connect through a HTTPS connection using the generated private key
            registry_url = json.loads(platform.config)['registry']
            registry_proxy = SfaServerProxy(registry_url, pkey_fn.name, cert_fn.name)

            try:
                credential_string=registry_proxy.GetCredential (config['user_credential'], config['user_hrn'].encode('latin1'), 'authority')
                config['authority_credential'] = credential_string
            except:
                pass # No authority credential

            os.unlink(pkey_fn.name)
            os.unlink(cert_fn.name)


        if new_key or not 'slice_credentials' in config:
            # Generated on demand !
            config['slice_credentials'] = {}

        return config
