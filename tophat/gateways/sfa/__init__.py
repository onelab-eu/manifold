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

from tophat.core.ast import FromNode
from tophat.util.faults import *

from tophat.core.filter import *
from tophat.core.metadata import Metadata
from tophat.gateways.sfa.rspecs.SFAv1 import SFAv1Parser as Parser

from sfa.trust.certificate import Keypair, Certificate
from sfa.trust.gid import GID
from sfa.trust.credential import Credential
# from sfa.trust.sfaticket import SfaTicket

from sfa.util.sfalogging import sfi_logger
from sfa.util.xrn import get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from sfa.util.config import Config
from sfa.util.version import version_core
from sfa.util.cache import Cache

from sfa.storage.record import Record

from sfa.rspecs.rspec import RSpec
#from sfa.rspecs.rspec_converter import RSpecConverter
from sfa.rspecs.version_manager import VersionManager

from sfa.client.sfaclientlib import SfaClientBootstrap
from sfa.client.client_helper import pg_users_arg, sfa_users_arg
from sfa.client.sfaserverproxy import SfaServerProxy, ServerException
from sfa.client.return_value import ReturnValue

import signal

DEMO_HOOKS = ['demo'] #, 'jordan.auge@lip6.fr']



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
        if self.bootstrap_done:
            return
        #bootstrap = SfaClientBootstrap (self.user, self.reg_url, self.config['sfi_dir'])
        bootstrap = SfaClientBootstrap (self.gateway_config['user'], self.reg_url, self.gateway_config['sfi_dir'])
        # if -k is provided, use this to initialize private key
        if self.gateway_config['user_private_key']:
            bootstrap.init_private_key_if_missing (self.gateway_config['user_private_key'])
        else:
            # trigger legacy compat code if needed 
            # the name has changed from just <leaf>.pkey to <hrn>.pkey
            if not os.path.isfile(bootstrap.private_key_filename()):
                self.logger.info ("private key not found, trying legacy name")
                try:
                    legacy_private_key = os.path.join (self.gateway_config['sfi_dir'], "%s.pkey"%get_leaf(self.gateway_config['user']))
                    self.logger.debug("legacy_private_key=%s"%legacy_private_key)
                    bootstrap.init_private_key_if_missing (legacy_private_key)
                    self.logger.info("Copied private key from legacy location %s"%legacy_private_key)
                except:
                    self.logger.log_exc("Can't find private key ")
                    sys.exit(1)
            
        # make it bootstrap
        bootstrap.bootstrap_my_gid()
        # extract what's needed
        self.private_key = bootstrap.private_key()
        self.my_credential_string = bootstrap.my_credential_string ()
        self.my_gid = bootstrap.my_gid ()
        self.bootstrap = bootstrap
        self.bootstrap_done = True



    #
    # Management of the servers
    # 

    def registry (self):
        if not self.bootstrap_done:
            self.bootstrap()
        # cache the result
        if not hasattr (self, 'registry_proxy'):
            self.logger.info("Contacting Registry at: %s"%self.reg_url)
            self.registry_proxy = SfaServerProxy(self.reg_url,
                    self.gateway_config['user_private_key'], self.my_gid, 
                    timeout=self.gateway_config['timeout'],
                    verbose=self.gateway_config['debug'])  
        return self.registry_proxy

    def sliceapi (self):
        if not self.bootstrap_done:
            self.bootstrap()
        # cache the result
        if not hasattr (self, 'sliceapi_proxy'):
            # if the command exposes the --component option, figure it's hostname and connect at CM_PORT
            #if hasattr(self.command_options,'component') and self.command_options.component:
            #    # resolve the hrn at the registry
            #    node_hrn = self.command_options.component
            #    records = self.registry().Resolve(node_hrn, self.my_credential_string)
            #    records = filter_records('node', records)
            #    if not records:
            #        self.logger.warning("No such component:%r"% opts.component)
            #    record = records[0]
            #    cm_url = "http://%s:%d/"%(record['hostname'],CM_PORT)
            #    self.sliceapi_proxy=SfaServerProxy(cm_url, self.private_key, self.my_gid)
            #else:
            # otherwise use what was provided as --sliceapi, or SFI_SM in the config
            if not self.sm_url.startswith('http://') or self.sm_url.startswith('https://'):
                self.sm_url = 'http://' + self.sm_url
            self.logger.info("Contacting Slice Manager at: %s"%self.sm_url)
            self.sliceapi_proxy = SfaServerProxy(self.sm_url,
                    self.gateway_config['user_private_key'], self.my_gid,
                    timeout=self.gateway_config['timeout'],
                    verbose=self.gateway_config['debug'])  
        return self.sliceapi_proxy

    def get_cached_server_version(self, server):
        # check local cache first
        cache = None
        version = None 
        cache_file = os.path.join(self.gateway_config['sfi_dir'],'sfi_cache.dat')
        cache_key = server.url + "-version"
        try:
            cache = Cache(cache_file)
        except IOError:
            cache = Cache()
            self.logger.info("Local cache not found at: %s" % cache_file)

        if cache:
            version = cache.get(cache_key)

        if not version: 
            result = server.GetVersion()
            version= ReturnValue.get_value(result)
            # cache version for 20 minutes
            cache.add(cache_key, version, ttl= 60*20)
            self.logger.info("Updating cache file %s" % cache_file)
            cache.save_to_file(cache_file)

        return version   
        
    ### resurrect this temporarily so we can support V1 aggregates for a while
    def server_supports_options_arg(self, server):
        """
        Returns true if server support the optional call_id arg, false otherwise. 
        """
        server_version = self.get_cached_server_version(server)
        result = False
        # xxx need to rewrite this 
        if int(server_version.get('geni_api')) >= 2:
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
        results = self.sliceapi().ListSlices(cred, *self.ois(self.sliceapi(),api_options)) # user cred
        results = results['value']
        #{'output': '', 'geni_api': 2, 'code': {'am_type': 'sfa', 'geni_code': 0, 'am_code': None}, 'value': [
        return [urn_to_hrn(r)[0] for r in results]

    def sfa_list_records(self, cred, hrns, record_type=None):
        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
            raise PLCInvalidArgument('Wrong filter in sfa_list')
        records = self.registry().List(hrns, cred)
        if record_type:
            records = filter_records(record_type, records)
        return records

    def sfa_resolve_records(self, cred, hrns, record_type=None):
        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
            raise PLCInvalidArgument('Wrong filter in sfa_list')

        try:
            records = self.registry().Resolve(hrns, cred)
        except Exception, why:
            print "[Sfa::sfa_resolve_records] ERROR : %s" % why
            return []

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
        if hrn:
            api_options['geni_slice_urn'] = hrn_to_urn(hrn, 'slice')
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

        result = self.sliceapi().ListResources(cred, api_options)
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
                server = self.registry()
            else:
                server = self.sliceapi()
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

    def build_sfa_rspec(self, resources, leases):
        parser = Parser(resources, leases)
        return parser.to_rspec()


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

    def get_slice(self, filters = None, params = None, fields = None):
        return self._get_slices(filters, Metadata.expand_output_fields('slices', fields))

    def update_slice(self, filters, params, fields):
        if 'resource' not in params:
            raise Exception, "Update failed: nothing to update"

        # Keys
        if not filters.has_eq('slice_hrn'):
            raise Exception, 'Missing parameter: slice_hrn'
        slice_hrn = filters.get_eq('slice_hrn')
        slice_urn = hrn_to_urn(slice_hrn, 'slice')
        resources = params['resource']
        leases = params['lease']

        # Credentials
        user_cred = self._get_cred('user')
        slice_cred = self._get_cred('slice', slice_hrn)

        # We suppose resource
        rspec = self.build_sfa_rspec(resources, leases)

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
        print "CONNECTING TO REGISTRY", self.registry()
        slice_records = self.registry().Resolve(slice_urn, [user_cred])
        # slice_records = self.registry().Resolve(slice_urn, [self.my_credential_string], {'details':True})
        if slice_records and 'reg-researchers' in slice_records[0] and slice_records[0]['reg-researchers']:
            slice_record = slice_records[0]
            user_hrns = slice_record['reg-researchers']
            user_urns = [hrn_to_urn(hrn, 'user') for hrn in user_hrns]
            user_records = self.registry().Resolve(user_urns, [user_cred])

            server_version = self.registry().GetVersion()
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
        result = self.sliceapi().CreateSliver(slice_urn, [slice_cred], rspec, users, *self.ois(self.sliceapi(), api_options))
        manifest = ReturnValue.get_value(result)

        rsrc_leases = self.parse_sfa_rspec(manifest)

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        return [slice]

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



    def _get_slices(self, filters = None, fields = None):

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
            #if subfields: # XXX Disabled until we have default subqueries
            if has_resources:
                rsrc_leases = self.get_resource({'slice_hrn': 'ple.upmc.agent'}, subfields)
                if not rsrc_leases:
                    raise Exception, 'get_resources failed!'
                s['resource'] = rsrc_leases['resource']
                s['lease'] = rsrc_leases['lease'] 
            if has_users:
                s['users'] = [{'person_hrn': 'myslice.demo'}]

            return [s]
        #
        # END: DEMO
        #

        if isinstance(filters, list): # tuple set
            pass
            # list of hrn !
            # list of filter: possible ?
        elif isinstance(filters, StringTypes):
            pass
            # hrn : infer type ?
        else:
            if filters and not isinstance(filters, Filter):
                raise Exception, "Unsupported input_filter type"
        
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
        filtered = project_select_and_rename_fields(slices, 'slice_hrn', filters, fields, self.map_slice_fields)
        # XXX generic function to manage subrequests
        
        # - Get the list of subfields
        has_resource = False
        has_lease = False
        for of in fields:
            if of == 'resource' or of.startswith('resource.'):
                has_resource = True
            if of == 'lease' or of.startswith('lease.'):
                has_lease = True

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
                rsrc_leases = self.get_resource({'slice_hrn': hrn}, subfields)
                if not rsrc_leases:
                    print "W: Could not collect resource/leases for slice %s" % hrn
                    #raise Exception, 'get_resources failed!'
                print "LEASES"
                print rsrc_leases
                if has_resource:
                    s['resource'] = rsrc_leases['resource']
                if has_lease:
                    s['lease'] = rsrc_leases['lease'] 

        # remove join fields
        if 'slice_hrn' not in fields:
            for s in filtered:
                del s['slice_hrn']

        return filtered

    def get_users(self, input_filter = None, output_fields = None):
        if not output_fields:
            output_fields = ['user_hrn', 'user_nodes_sliver', 'user_nodes_all']
        return self._get_users(input_filter, output_fields)

    def get_user(self, input_filter = None, output_fields = None):
        if not output_fields:
            output_fields = ['user_hrn', 'user_first_name', 'user_last_name']
        return self._get_users(input_filter, output_fields)

    def _get_users(self, filters = None, fields = None):

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
                ul = self.registry().List(hrn, cred)
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

            users = self.registry().Resolve(user_list, cred)
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
                    records = self.registry().List(hrn, user_cred)
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
#        return self.sliceapi().SliverStatus(slice_urn, creds)

    def get_resource(self, input_filter = None, output_fields = None):
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
        
            rspec = self.sfa_get_resources(cred, hrn)
            if hrn:
                self.add_rspec_to_cache(hrn, rspec)
            return self.parse_sfa_rspec(rspec)
        except Exception, e:
            print "E: get_resource", e
            return []

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

    def __init__(self, router, platform, query, gateway_config, user_config, user):
#        FromNode.__init__(self, platform, query, config)
        super(SFA, self).__init__(router, platform, query, gateway_config, user_config, user)

        # self.config has always ['caller']

        # Check the presence of mandatory fields, default others
        #if not 'hashrequest' in self.config:    
        #    self.config['hashrequest'] = False
        #if not 'protocol' in self.config:
        #    self.config['protocol'] = 'xmlrpc'
        if not 'verbose' in self.gateway_config:
            self.gateway_config['verbose'] = 0
        if not 'auth' in self.gateway_config:
            raise Exception, "Missing SFA::auth parameter in configuration."
        if not 'user' in self.gateway_config:
            raise Exception, "Missing SFA::user parameter in configuration."
        if not 'sm' in self.gateway_config:
            raise Exception, "Missing SFA::sm parameter in configuration."
        if not 'debug' in self.gateway_config:
            self.gateway_config['debug'] = False

        # XXX this should be removed
        if not 'sfi_dir' in self.gateway_config:
            sfi_platform = self.user_config['reference_platform'] if 'reference_platform' in self.user_config else platform
            self.gateway_config['sfi_dir'] = '/var/myslice/%s/' % sfi_platform

        if not 'registry' in self.gateway_config:
            raise Exception, "Missing SFA::registry parameter in configuration."
        if not 'timeout' in self.gateway_config:
            self.gateway_config['timeout'] = None
        if not 'user_private_key' in self.gateway_config:
            raise Exception, "Missing SFA::user_private_key parameter in configuration."

        # XXX all this is redundant
        self.logger = sfi_logger
        self.reg_url = self.gateway_config['registry']
        self.sm_url = self.gateway_config['sm']

        self.bootstrap_done = False

    def __str__(self):
        return "<SFAGateway %r: %s>" % (self.gateway_config['sm'], self.query)

    def do_start(self):
        q = self.query
        # Let's call the simplest query as possible to begin with
        # This should use twisted XMLRPC
        result = getattr(self, "%s_%s" % (q.action, q.fact_table))(q.filters, q.params, list(q.fields))
        for r in result:
            self.callback(r)
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
        print "USER HRN=", config['user_hrn']

        if not 'user_private_key' in config:
            print "I: Generating user private key"
            k = Keypair(create=True)
            config['user_public_key'] = k.get_pubkey_string()
            config['user_private_key'] = k.as_pem()
            new_key = True

        if new_key or not 'sscert' in config or not config['sscert']:
            keypair = Keypair(string=config['user_private_key'])
            self_signed = Certificate(subject = config['user_hrn'])
            self_signed.set_pubkey(keypair)
            self_signed.set_issuer(keypair, subject=config['user_hrn'].encode('latin1'))
            self_signed.sign()
            config['sscert'] = self_signed.save_to_string()

        if new_key or not 'user_credential' in config: # or expired
            # Create temporary files for key and certificate in order to use existing code based on httplib
            pkey_fn = tempfile.NamedTemporaryFile(delete=False)
            pkey_fn.write(config['user_private_key'])
            cert_fn = tempfile.NamedTemporaryFile(delete=False)
            cert_fn.write(config['sscert'])
            pkey_fn.close()
            cert_fn.close()

            # We need to connect through a HTTPS connection using the generated private key
            registry_url = json.loads(platform.config)['registry_url']
            registry_proxy = SfaServerProxy (registry_url, pkey_fn.name, cert_fn.name)

            os.unlink(pkey_fn.name)
            os.unlink(cert_fn.name)

            try:
                credential_string = registry_proxy.GetSelfCredential (config['sscert'], config['user_hrn'], 'user')
            except:
                # some urns hrns may replace non hierarchy delimiters '.' with an '_' instead of escaping the '.'
                hrn = Xrn(config['user_hrn']).get_hrn().replace('\.', '_')
                credential_string=registry_proxy.GetSelfCredential (config['sscert'], hrn, 'user')

            config['user_credential'] = credential_string

        if new_key or not 'gid' in config:
            # Create temporary files for key and certificate in order to use existing code based on httplib
            pkey_fn = tempfile.NamedTemporaryFile(delete=False)
            pkey_fn.write(config['user_private_key'])
            cert_fn = tempfile.NamedTemporaryFile(delete=False)
            cert_fn.write(config['sscert'])
            pkey_fn.close()
            cert_fn.close()

            # We need to connect through a HTTPS connection using the generated private key
            registry_url = json.loads(platform.config)['registry_url']
            registry_proxy = SfaServerProxy(registry_url, pkey_fn.name, cert_fn.name)

            os.unlink(pkey_fn.name)
            os.unlink(cert_fn.name)

            records = registry_proxy.Resolve(hrn, config['user_credential'])
            records = [record for record in records if record['type']=='user']
            if not records:
                raise RecordNotFound, "hrn %s (%s) unknown to registry %s"%(hrn,type,self.registry_url)

            record = records[0]
            config['gid'] = record['gid']

        if new_key or not 'slice_credentials' in config:
            # Generated on demand !
            config['slice_credentials'] = {}

        return config

#def sfa_get(api, caller, method, ts, input_filter = None, output_fields = None):
#    sfa = Sfa(api, caller)
#    # XXX select project and rename (networks, slices, users)
#    return getattr(sfa, "get_%s" % method)(input_filter, output_fields)
#
#def sfa_update(api, caller, method, ts, input_filter = None, output_fields = None):
#    sfa = Sfa(api, caller)
#
#    return getattr(sfa, "update_%s" % method)(input_filter, output_fields)
