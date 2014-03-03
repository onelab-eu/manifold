import sys, os, os.path, re, tempfile, itertools
import zlib, hashlib, BeautifulSoup, urllib
import json, signal, traceback, time
from datetime                           import datetime
from lxml                               import etree
from StringIO                           import StringIO
from types                              import StringTypes, ListType
from twisted.internet                   import defer

from manifold.conf                      import ADMIN_USER
from manifold.core.result_value         import ResultValue
from manifold.core.filter               import Filter
from manifold.core.record               import Record, Records, LastRecord
from manifold.operators.rename          import Rename
from manifold.gateways                  import Gateway
#from manifold.gateways.sfa.rspecs.SFAv1 import SFAv1Parser # as Parser
from manifold.gateways.sfa.proxy        import SFAProxy
from manifold.util.predicate            import contains, eq, lt, le, included
from manifold.util.log                  import Log
from manifold.util.misc                 import make_list
from manifold.models                    import db
from manifold.models.platform           import Platform 
from manifold.models.user               import User

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

AM_SLICE_FIELDS = set(['resource', 'lease'])

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

################################################################################
# Helper functions
################################################################################

def filter_records(type, records):
    filtered_records = []
    for record in records:
        if (record['type'] == type) or (type == "all"):
            filtered_records.append(record)
    return filtered_records

# XXX This function should disappear since we have AST
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
                if not fields or k in fields: #  or k == pkey:
                    c[k] = v
            if 'slice_urn' in row and ('slice_hrn' in fields or not fields):
                c['slice_hrn'] = urn_to_hrn(row['slice_urn'])[0]
            filtered.append(c)

    return filtered

################################################################################

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
    __gateway_name__ = 'sfa'

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
        'researchers'       : 'user.user_hrn',              # This should be 'users.hrn'
        'reg-urn'           : 'slice_urn',                  # slice_geni_urn ???
        'site_id'           : 'slice_site_id',              # X ID 
        'site'              : 'slice_site',                 # authority.hrn
        'authority'         : 'parent_authority',       # isn't it the same ???
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
        'authority': 'parent_authority',               # authority it belongs to
        'peer_authority': 'user_peer_authority',    # ?
        'hrn': 'user_hrn',                          # hrn
        'gid': 'user_gid',                          # gif
        'type': 'user_type',                        # type ???
        'last_updated': 'user_last_updated',        # last_updated
        'date_created': 'user_date_created',        # first
        'email':  'user_email',                     # email
        'first_name': 'user_first_name',            # first_name
        'last_name': 'user_last_name',              # last_name
        'phone': 'user_phone',                      # phone
        #'keys': 'user_keys',                       # OBJ keys !!!
        'reg-keys': 'keys',                         # OBJ keys !!!
        'reg-slices': 'slice.slice_hrn',            # OBJ slices
        'reg-pi-authorities': 'pi_authorities',
    }

    map_authority_fields = {
        'hrn'               : 'authority_hrn',                  # hrn
        'PI'                : 'pi_users',
#        'persons'           : 'user',
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
            defer.returnValue((None, None))
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
            defer.returnValue((account.auth_type, json.loads(new_user_config)))
        else:
            defer.returnValue((account.auth_type, json.loads(account.config)))
        #return json.loads(new_user_config) if new_user_config else None

    def make_user_proxy(self, interface_url, user_config, cert_type='gid'):
        """
        interface (string): 'registry', 'sm' or URL
        user_config (dict): user configuration
        cert_type (string): 'gid', 'sscert'
        """
        pkey    = user_config['user_private_key'].encode('latin1')
        # default is gid, if we don't have it (see manage function) we use self signed certificate
        cert    = user_config[cert_type]
        timeout = self.config['timeout']

        if not interface_url.startswith('http://') and not interface_url.startswith('https://'):
            interface_url = 'http://' + interface_url

        return SFAProxy(interface_url, pkey, cert, timeout)
    
    # init self-signed cert, user credentials and gid
    @defer.inlineCallbacks
    def bootstrap (self):
        # Cache admin config
        _, self.admin_config = yield self.get_user_config(ADMIN_USER)
        assert self.admin_config, "Could not retrieve admin config"

        # Overwrite user config (reference & managed acccounts)
        new_auth_type, new_user_config = yield self.get_user_config(self.user['email'])
        if new_user_config:
            self.auth_type   = new_auth_type
            self.user_config = new_user_config
        else:
            self.auth_type   = None

        # Initialize manager proxies using MySlice Admin account
        try:
            if self.config['registry']:
                self.registry = self.make_user_proxy(self.config['registry'], self.admin_config)
                registry_hrn = yield self.get_interface_hrn(self.registry)
                self.registry.set_network_hrn(registry_hrn)

            if self.config['sm']:
                self.sliceapi = self.make_user_proxy(self.config['sm'],       self.admin_config)
                sm_hrn = yield self.get_interface_hrn(self.sliceapi)
                self.sliceapi.set_network_hrn(sm_hrn)

        except Exception, e:
            print "EXC in boostrap", e
            import traceback
            traceback.print_exc()
            



    def is_admin(self, user):
        if isinstance(user, StringTypes):
            return user == ADMIN_USER
        else:
            return user['email'] == ADMIN_USER

    @defer.inlineCallbacks
    def get_cached_server_version(self, server):
        # check local cache first
        version = None 
        cache_key = server.get_interface_hrn() + "-version"
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
        #if version['interface'] == 'registry':
        if 'interface' in version and version['interface'] == 'registry':
            self.registry_version = version
        else:
            self.am_version = version

        defer.returnValue(version)

    @defer.inlineCallbacks
    def get_interface_hrn(self, server):
        server_version = yield self.get_cached_server_version(server)    
        defer.returnValue(server_version['hrn'])
        
    ### resurrect this temporarily so we can support V1 aggregates for a while
    @defer.inlineCallbacks
    def server_supports_options_arg(self, server):
        """
        Returns true if server support the optional call_id arg, false otherwise. 
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
    def ois (self, server, option_dict):
        flag = yield self.server_supports_options_arg(server)
        if flag:
            defer.returnValue(option_dict)
        else:
            flag = yield self.server_supports_call_id_arg(server)
            if flag:
                defer.returnValue([unique_call_id()])
            else:
                defer.returnValue([])

    ### cis = call_id if supported - like ois
    @defer.inlineCallbacks
    def cis (self, server):
        flag = yield self.server_supports_call_id_arg(server)
        if flag:
            defer.returnValue([unique_call_id()])
        else:
            defer.returnValue([])

    ############################################################################ 
    #
    # SFA Method wrappers
    #
    ############################################################################ 

    def sfa_list_records(self, cred, hrns, record_type=None):
        if record_type not in [None, 'user', 'slice', 'authority', 'node']:
            raise Exception('Wrong filter in sfa_list')
        records = self.registry.List(hrns, cred)
        if record_type:
            records = filter_records(record_type, records)
        return records

    ########################################################################### 
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
            result = server.GetVersion(timeout=DEFAULT_TIMEOUT_GETVERSION)
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
                server = self.make_user_proxy(interface, self.user_config)
                try:
                    version = ReturnValue.get_value(server.GetVersion(timeout=DEFAULT_TIMEOUT_GETVERSION))
                except Exception, why:
                    print "E: ", why
                    version = None
                    print traceback.print_exc()

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

    def make_dict_rec(self, obj):
        if not obj or isinstance(obj, (StringTypes, bool)):
            return obj
        if isinstance(obj, list):
            objcopy = []
            for x in obj:
                objcopy.append(self.make_dict_rec(x))
            return objcopy
        # We thus suppose we have a child of dict
        objcopy = {}
        for k, v in obj.items():
            objcopy[k] = self.make_dict_rec(v)
        return objcopy

    def parse_sfa_rspec(self, rspec_string):
        # rspec_type and rspec_version should be set in the config of the platform,
        # we use GENIv3 as default one if not
        if 'rspec_type' and 'rspec_version' in self.config:
            rspec_version = self.config['rspec_type'] + ' ' + self.config['rspec_version']
        else:
            rspec_version = 'GENI 3'
            #rspec_version = 'SFA 1'
        Log.debug(rspec_version)
        rspec = RSpec(rspec_string, version=rspec_version)

        resources = [] 
# These are all resources 
# get_resources function can return all resources or a specific type of resource
        try:
            resources = rspec.version.get_resources()
        except Exception, e:
            Log.warning("Could not retrieve resources in RSpec: %s" % e)
        
        # XXX does not scale... we need get_resources and that's all
        try:
            nodes = rspec.version.get_nodes()
        except Exception, e:
            nodes = list()
            Log.warning("Could not retrieve nodes in RSpec: %s" % e)
        try:
            leases = rspec.version.get_leases()
        except Exception, e:
            leases = list()
            Log.warning("Could not retrieve leases in RSpec: %s" % e)
        try:
            links = rspec.version.get_links()
        except Exception, e:
            links = list()
            Log.warning("Could not retrieve links in RSpec: %s" % e)
        try:
            channels = rspec.version.get_channels()
        except Exception, e:
            channels = list()
            Log.warning("Could not retrieve channels in RSpec: %s" % e)

        # Extend object and Format object field's name
        for resource in resources:
            resource['urn'] = resource['component_id']

        for node in nodes:
            node['type'] = 'node'
            node['network_hrn'] = Xrn(node['component_id']).authority[0] # network ? XXX
            node['hrn'] = urn_to_hrn(node['component_id'])[0]
            node['urn'] = node['component_id']
            node['hostname'] = node['component_name']
            node['initscripts'] = node.pop('pl_initscripts')
            if 'exclusive' in node and node['exclusive']:
                node['exclusive'] = node['exclusive'].lower() == 'true'

            # XXX This should use a MAP as before
            if 'position' in node: # iotlab
                node['x'] = node['position']['posx']
                node['y'] = node['position']['posy']
                node['z'] = node['position']['posz']
                del node['position']

            if 'location' in node:
                if node['location']:
                    node['latitude'] = node['location']['latitude']
                    node['longitude'] = node['location']['longitude']
                del node['location']

            # Flatten tags
            if 'tags' in node:
                if node['tags']:
                    for tag in node['tags']:
                        node[tag['tagname']] = tag['value']
                del node['tags']

            
            # We suppose we have children of dict that cannot be serialized
            # with xmlrpc, let's make dict
            resources.append(self.make_dict_rec(node))

        # NOTE a channel is a resource and should not be treated independently
        #     resource
        #        |
        #   +----+------+-------+
        #   |    |      |       |
        # node  link  channel  etc.
        #resources.extend(nodes)
        #resources.extend(channels)

        for lease in leases:
            lease['resource'] = lease.pop('component_id')
            lease['slice']    = lease.pop('slice_id')

        print "resources=", resources
        print "leases=", leases
        return {'resource': resources, 'lease': leases } 
#               'channel': channels \
#               }

    def manifold_to_sfa_leases(self, leases, slice_id):
        sfa_leases = []
        for lease in leases:
            sfa_lease = dict()
            # sfa_lease_id = 
            sfa_lease['component_id'] = lease['resource']
            sfa_lease['slice_id']     = slice_id
            sfa_lease['start_time']   = lease['start_time']
            sfa_lease['duration']   = lease['duration']
            sfa_leases.append(sfa_lease)
        return sfa_leases
    

    def build_sfa_rspec(self, slice_id, resources, leases):
        #if isinstance(resources, str):
        #    resources = eval(resources)
        # rspec_type and rspec_version should be set in the config of the platform,
        # we use GENIv3 as default one if not
        if 'rspec_type' and 'rspec_version' in self.config:
            rspec_version = self.config['rspec_type'] + ' ' + self.config['rspec_version']
        else:
            rspec_version = 'GENI 3'

        # extend rspec version with "content_type"
        rspec_version += ' request'
        
        rspec = RSpec(version=rspec_version)

        nodes = []
        channels = []
        links = []

        # XXX Here it is only about mappings and hooks between ontologies

        for urn in resources:
            # XXX TO BE CORRECTED, this handles None values
            if not urn:
                continue
            resource = dict()
            # TODO: take into account the case where we send a dict of URNs without keys
            #resource['component_id'] = resource.pop('urn')
            resource['component_id'] = urn
            resource_hrn, resource_type = urn_to_hrn(urn) # resource['component_id'])
            # build component_manager_id
            top_auth = resource_hrn.split('.')[0]
            cm = urn.split("+")
            resource['component_manager_id'] = "%s+%s+authority+cm" % (cm[0],top_auth)

            if resource_type == 'node':
                # XXX dirty hack WiLab !!!
                if 'wilab2' in self.config['sm']:
                    resource['client_id'] = "PC"
                    resource['sliver_type'] = "raw-pc"
                nodes.append(resource)
            elif resource_type == 'link':
                links.append(resource)
            elif resource_type == 'channel':
                channels.append(resource)
            else:
                raise Exception, "Not supported type of resource" 
        
        rspec.version.add_nodes(nodes, rspec_content_type="request")
        sfa_leases = self.manifold_to_sfa_leases(leases, slice_id)
        rspec.version.add_leases(sfa_leases)
        #rspec.version.add_links(links)
        #rspec.version.add_channels(channels)
   
        Log.warning("request rspec: %s"%rspec.toxml())
        return rspec.toxml()

    ############################################################################ 
    #
    # COMMANDS
    #
    ############################################################################ 

    def _get_cred(self, type, target = None, v3 = False):
        if v3:
            return {
                'geni_version': '3',
                'geni_type': 'geni_sfa',
                'geni_value': self.__get_cred(type, target) #.encode('latin-1')
            }
        else:
            return self.__get_cred(type, target)


    # get a delegated credential of a given type to a specific target
    # default allows the use of MySlice's own credentials
    def __get_cred(self, type, target=None):
        cred = None
        delegated='delegated_' if not self.is_admin(self.user) else ''
        Log.debug('Get Credential for %s = %s'% (type,target))           
        if type == 'user':
            if target:
                raise Exception, "Cannot retrieve specific user credential for now"
            try:
                return self.user_config['%suser_credential'%delegated]
            except TypeError, e:
                raise Exception, "Missing user credential %s" %  str(e)
        elif type in ['authority', 'slice']:
            if not '%s%s_credentials' % (delegated, type) in self.user_config:
                self.user_config['%s%s_credentials' % (delegated, type)] = {}

            creds = self.user_config['%s%s_credentials' % (delegated, type)]
            if target in creds:
                cred = creds[target]
            else:
                # Can we generate them : only if we have the user private key
                # Currently it is not possible to request for a slice/authority credential
                # with a delegated user credential...
                if 'user_private_key' in self.user_config and self.user_config['user_private_key'] and type == 'slice':
                    cred = SFAGateway.generate_slice_credential(target, self.user_config)
                    creds[target] = cred
                # If user has an authority credential above the one targeted
                # Example: 
                # target = ple.inria / user is a PLE Admin and has creds = [ple.upmc , ple]
                # if ple.inria starts with ple then let's use the ple credential
                elif type == 'authority':
                    for my_auth in creds:
                        if target.startswith(my_auth):
                            cred=creds[my_auth]
                    if not cred:
                        raise Exception , "no cred found of type %s towards %s " % (type, target)
                else:
                    raise Exception , "no cred found of type %s towards %s " % (type, target)
            return cred
        else:
            raise Exception, "Invalid credential type: %s" % type


    @defer.inlineCallbacks
    def update_slice_am(self, filters, params, fields):
        if not 'resource' in params and not 'lease' in params:
            raise Exception, "Update failed: nothing to update"
        
        # Need to be specified to remain unchanged
        need_resources = not 'resource' in params
        need_leases    = not 'lease'    in params

        # XXX We also need to add leases
        #print "begin get rl **", filters, "**"
        if need_resources or need_leases:
            resource_lease = yield self.get_resource_lease(filters, None, fields, list_resources = need_resources, list_leases = need_leases)
            #print "end get rl"
            # XXX Need to handle +=
            if need_resources:
                params['resource'] = [r['urn'] for r in resource_lease['resource']]
            if need_leases:
                params['lease'] = resource_lease['lease']

        for lease in params['lease']:
            resource_urn = lease['resource']
            # XXX We might have dicts, we need helper functions...
            if not resource_urn in params['resource']:
                params['resource'].append(lease['resource'])

        #print "RESOURCES", len(params['resource'])
        #print "LEASES:", params['lease']

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
        print "build rspec"
        rspec = self.build_sfa_rspec(slice_urn, resources, leases)
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

            geni_users = pg_users_arg(user_records)
            sfa_users = sfa_users_arg(user_records, slice_record)
            if 'sfa' not in server_version:
                #print "W: converting to pg rspec"
                users = geni_users
                #rspec = RSpec(rspec)
                #rspec.filter({'component_manager_id': server_version['urn']})
                #rspec = RSpecConverter.to_pg_rspec(rspec.toxml(), content_type='request')
            else:
                users = sfa_users
                
        # do not append users, keys, or slice tags. Anything
        # not contained in this request will be removed from the slice

        # CreateSliver has supported the options argument for a while now so it should
        # be safe to assume this server support it
        api_options = {}
        api_options ['append'] = False
        api_options ['call_id'] = unique_call_id()

        # Manage Rspec versions
        if 'rspec_type' and 'rspec_version' in self.config:
            api_options['geni_rspec_version'] = {'type': self.config['rspec_type'], 'version': self.config['rspec_version']}
        else:
            # For now, lets use GENIv3 as default
            api_options['geni_rspec_version'] = {'type': 'GENI', 'version': '3'}
            #api_options['geni_rspec_version'] = {'type': 'SFA', 'version': '1'}  

        if self.am_version['geni_api'] == 2:
            # AM API v2
            ois = yield self.ois(self.sliceapi, api_options)
            result = yield self.sliceapi.CreateSliver(slice_urn, [slice_cred], rspec, users, ois)
            Log.warning("CreateSliver Result: %s" %result)
        else:
            # AM API v3
            api_options['sfa_users'] = sfa_users
            api_options['geni_users'] = geni_users

            result = yield self.sliceapi.Allocate(slice_urn, [slice_cred], rspec, api_options)
            result = yield self.sliceapi.Provision([slice_urn], [slice_cred], api_options)

        manifest = ReturnValue.get_value(result)

        if not manifest:
            print "NO MANIFEST FROM", self.platform, result
            defer.returnValue([])
        else:
            print "GOT MANIFEST FROM", self.platform
            print "MANIFEST=", manifest
            sys.stdout.flush()


        if self.am_version['geni_api'] == 2:
            rsrc_leases = self.parse_sfa_rspec(manifest)
        else:
            # AM API v3
            # 
            rsrc_leases = self.parse_sfa_rspec(manifest['geni_rspec'])

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        #print "oK"
        #print "SLICE=", slice
        defer.returnValue([slice])

    # This function will return information about a given network using SFA GetVersion call
    # Depending on the object Queried, if object is network then get_network is triggered by
    # result = getattr(self, "%s_%s" % (q.action, q.object))(local_filters, q.params, fields)
    @defer.inlineCallbacks
    def get_network(self, filters = None, params = None, fields = None):
        Log.debug(self.sliceapi)
        # Network (AM) 
        if self.sliceapi:
            server = self.sliceapi
        # Network (Registry) 
        else:
            server = self.registry 

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
            output['platform']=self.platform

        output['version'] = version
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

    def get_slice_demo(self, filters, params, fields):
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
                rsrc_leases = self.get_resource_lease({'slice_hrn': 'ple.upmc.agent'}, subfields)
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
    def get_object(self, object, object_hrn, filters, params, fields):
        # Let's find some additional information in filters in order to restrict our research
        object_name = make_list(filters.get_op(object_hrn, [eq, included]))
        auth_hrn = make_list(filters.get_op('parent_authority', [eq, lt, le]))
        interface_hrn    = yield self.get_interface_hrn(self.registry)
        
        # XXX Hack for avoiding multiple calls to the same registry...
        # This will be fixed in newer versions where AM and RM have separate gateways
        if self.auth_type == "reference":
            # We could check for the "reference_platform" entry in
            # self.user_config but it seems in some configurations it has been
            # erased by credentials... weird
            defer.returnValue([])

        # XXX details = True always, only trigger details if needed wrt fields

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
            auth_hrn  = [a for a in auth_hrn if a.startswith(interface_hrn)]
            if not auth_hrn:
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
            recursive = True if object != 'authority' else False
            print "RECURSIVE=", recursive
            stack = [interface_hrn]
        
        # TODO: user's objects, use reg-researcher
        
        cred = self._get_cred('user')


        if resolve:
            stack = map(lambda x: hrn_to_urn(x, object), stack)
            _results  = yield self.registry.Resolve(stack, cred, {'details': True})
            #_result = _results[0]

            output = []
            for _result in _results:

                # XXX How to better handle DateTime XMLRPC types into the answer ?
                # XXX Shall we type the results like we do in CSV ?
                result = {}
                for k, v in _result.items():
                    if isinstance(v, DateTime):
                        result[k] = str(v) # datetime.strptime(str(v), "%Y%m%dT%H:%M:%S") 
                    else:
                        result[k] = v

                output.append(result)
 
            defer.returnValue(output)
        
        print "STACK=", stack
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

            started = time.time()
            records = yield self.registry.List(auth_xrn, cred, {'recursive': recursive})
            print "LIST:", time.time() - started, "s"

            records = [r for r in records if r['type'] == object]
            record_urns = [hrn_to_urn(record['hrn'], object) for record in records]
            Log.tmp(record_urns)
            # INSERT ROOT AUTHORITY
            if object == 'authority':
                record_urns.insert(0,hrn_to_urn(interface_hrn, object))

            started = time.time()
            records = yield self.registry.Resolve(record_urns, cred, {'details': True}) 
            print "RESOLVE:", time.time() - started, "s"
            Log.tmp(records[1])
            defer.returnValue(records)

    def get_slice(self, filters, params, fields):
        # XXX Sometimes we don't need to call for the registry

        if self.user['email'] in DEMO_HOOKS:
            defer.returnValue(self.get_slice_demo(filters, params, fields))
            return

        return self.get_object('slice', 'slice_hrn', filters, params, fields)

    def get_user(self, filters, params, fields):

        if self.user['email'] in DEMO_HOOKS:
            Log.tmp(self.user)
        #    defer.returnValue(self.get_user_demo(filters, params, fields))
        #    return

        return self.get_object('user', 'user_hrn', filters, params, fields)

    def get_authority(self, filters, params, fields):

        #if self.user['email'] in DEMO_HOOKS:
        #    defer.returnValue(self.get_authority_demo(filters, params, fields))
        #    return

        return self.get_object('authority', 'authority_hrn', filters, params, fields)


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
#DEPRECATED#        cred = self._get_cred('user')
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

    @defer.inlineCallbacks
    def create_object(self, filters, params, fields):
        # XXX should call create_record_from_params which would rely on mappings
        dict_filters = filters.to_dict()
        if self.query.object + '_hrn' in params:
            object_hrn = params[self.query.object+'_hrn']
        else:         
            object_hrn = params['hrn']
        if 'hrn' not in params:
            params['hrn'] = object_hrn
        if 'type' not in params:
            params['type'] = self.query.object
            #raise Exception, "Missing type in params"
        object_auth_hrn = get_authority(object_hrn)

        server_version = yield self.get_cached_server_version(self.registry)    
        server_auth_hrn = server_version['hrn']
        
        if not params['hrn'].startswith('%s.' % server_auth_hrn):
            # XXX not a success, neither a warning !!
            print "I: Not requesting object creation on %s for %s" % (server_auth_hrn, params['hrn'])
            defer.returnValue([])

        auth_cred = self._get_cred('authority', object_auth_hrn)

        if 'type' not in params:
            raise Exception, "Missing type in params"
        try:
            object_gid = yield self.registry.Register(params, auth_cred)
        except Exception, e:
            raise Exception, 'Failed to create object: record possibly already exists: %s' % e
        defer.returnValue([{'hrn': params['hrn'], 'gid': object_gid}])

    def create_user(self, filters, params, fields):
        return self.create_object(filters, params, fields)
 
    def create_slice(self, filters, params, fields):
        return self.create_object(filters, params, fields)

    def create_resource(self, filters, params, fields):
        return self.create_object(filters, params, fields)

    def create_authority(self, filters, params, fields):
        return self.create_object(filters, params, fields)

        ## Get the slice name
        #if not 'hrn' in params:
        #    raise Exception, "Create slice requires a slice name"
        #hrn = params['hrn']
        #
        ## Are we creating the slice on the right authority
        #slice_auth = get_authority(slice_hrn)
        #server_version = self.get_cached_server_version(self.registry)
        #server_auth = server_version['hrn']
        #if not slice_auth.startswith('%s.' % server_auth):
        #    print "I: Not requesting slice creation on %s for %s" % (server_auth, slice_hrn)
        #    return []
        #print "I: Requesting slice creation on %s for %s" % (server_auth, slice_hrn)
        #print "W: need to check slice is created under user authority"
        #cred = self._get_cred('authority')
        #record_dict = self.create_record_from_params('slice', params)
        #try:
        #    slice_gid = self.registry.Register(record_dict, cred)
        #except Exception, e:
        #    print "E: %s" % e
        #return []

    @defer.inlineCallbacks
    def update_object(self, filters, params, fields):
        # XXX should call create_record_from_params which would rely on mappings
        dict_filters = filters.to_dict()
        if filters.has(self.query.object+'_hrn'):
            object_hrn = dict_filters[self.query.object+'_hrn']
        else:
            object_hrn = dict_filters['hrn']
        if 'hrn' not in params:
            params['hrn'] = object_hrn
        if 'type' not in params:
            params['type'] = self.query.object
            #raise Exception, "Missing type in params"
        object_auth_hrn = get_authority(object_hrn)
        server_version = yield self.get_cached_server_version(self.registry)
        server_auth_hrn = server_version['hrn']
        if not object_auth_hrn.startswith('%s.' % server_auth_hrn):
            # XXX not a success, neither a warning !!
            print "I: Not requesting object update on %s for %s" % (server_auth_hrn, object_auth_hrn)
            defer.returnValue([])
        # If we update our own user, we only need our user_cred
        if self.query.object == 'user':
            try:
                caller_user_hrn = self.user_config['user_hrn']
            except Exception, e:
                raise Exception, "Missing user_hrn in account.config of the user" 
            if object_hrn == caller_user_hrn:
                Log.tmp("Need a user credential to update your own user: %s" % object_hrn)
                auth_cred = self._get_cred('user')
            # Else we need an authority cred above the object
            else:
                Log.tmp("Need an authority credential to update another user: %s" % object_hrn)
                auth_cred = self._get_cred('authority', object_auth_hrn)
        else:
            Log.tmp("Need an authority credential to update: %s" % object_hrn)
            auth_cred = self._get_cred('authority', object_auth_hrn)
        try:
            object_gid = yield self.registry.Update(params, auth_cred)
        except Exception, e:
            raise Exception, 'Failed to Update object: %s' % e
        defer.returnValue([{'hrn': params['hrn'], 'gid': object_gid}])
    
    def update_user(self, filters, params, fields):
       return self.update_object(filters, params, fields)
    
    def update_slice(self, filters, params, fields):
        do_update_am = bool(set(params.keys()) & AM_SLICE_FIELDS)
        do_update_rm = bool(set(params.keys()) - AM_SLICE_FIELDS)

        do_get_am    = fields & AM_SLICE_FIELDS and not do_update_am
        do_get_rm    = fields - AM_SLICE_FIELDS and not do_update_rm

        do_am        = do_get_am or do_update_am
        do_rm        = do_get_rm or do_update_rm

        if do_am and do_rm:
            # Part on the RM side, part on the AM side... until AM and RM are
            # two different GW, we need to manually make a left join between
            # the results of both calls
            
            # Ensure join key in fields (in fact not needed since we filter on pkey)
            #has_key = 'slice_urn' in fields
            fields_am = fields & AM_SLICE_FIELDS
            #if not has_key:
            #     fields_am |= 'slice_urn'
            fields_rm = fields - AM_SLICE_FIELDS
            #if not has_key:
            #    fields_rm |= 'slice_urn'

            if do_get_am: # then we have do_update_rm (because update_slice)
                print "do get am"
                ret_am = self.get_slice(filters, params, fields_am)
                ret_rm = self.update_object(filters, params, fields_rm)
            else:
                print "do get rm"
                ret_am = self.update_slice_am(filters, params, fields_am)
                ret_rm = self.get_slice(filters, params, fields_rm)

            print "ret_am", ret_am
            print "ret_rm", ret_rm

            dl = defer.DeferredList([ret_am, ret_rm])
            if do_get_am:
                def cb(result):
                    assert len(result) == 2
                    (am_success, am_records), (rm_success, rm_records) = result
                    # XXX success
                    am_record = am_records[0]
                    rm_record = rm_records[0]
                    rm_record.update(am_record)
                    return [rm_record]
                dl.addCallback(cb)
                return dl
            else:
                def cb(result):
                    assert len(result) == 2
                    (am_success, am_records), (rm_success, rm_records) = result
                    # XXX success
                    am_record = am_records[0]
                    rm_record = rm_records[0]
                    am_record.update(rm_record)
                    return [am_record]
                dl.addCallback(cb)
                return dl

            # Remove key
            #if not has_key:
            #    del ret['slice_urn']

        if do_update_am:
            return self.update_slice_am(filters, params, fields)
        else: # do_update_rm
            return self.update_object(filters, params, fields)
        
    def update_authority(self, filters, params, fields):
        return self.update_object(filters, params, fields)

    # Let's not have resource in the registry for the time being since it causes conflicts with the AM until AM and RM are separated...
    #def update_resource(self, filters, params, fields):
    #    return self.update_object(filters, params, fields)

    # NOTE : The two following subqueries should be sent at the same time
    # Maintain pending queries ?
    # This was solved before thanks to update_slice

    def update_resource(self, filters, params, fields): # AM
        pass

    def update_leases(self, filters, params, fields): # AM
        pass

    # DELETE - REMOVE sent to the Registry
    # XXX TODO: What about Delete sent to the Registry???
    # To be implemented in ROUTERV2

    @defer.inlineCallbacks
    def delete_object(self, filters):
        dict_filters = filters.to_dict()
        if filters.has(self.query.object+'_hrn'):
            object_hrn = dict_filters[self.query.object+'_hrn']
        else:
            object_hrn = dict_filters['hrn']

        object_type = self.query.object
        object_auth_hrn = get_authority(object_hrn)
        Log.tmp("Need an authority credential to Remove: %s" % object_hrn)
        auth_cred = self._get_cred('authority', object_auth_hrn)
       
        try:
            Log.tmp(object_hrn, auth_cred, object_type)
            object_gid = yield self.registry.Remove(object_hrn, auth_cred, object_type)
        except Exception, e:
            raise Exception, 'Failed to Remove object: %s' % e
        defer.returnValue([{'hrn': object_hrn, 'gid': object_gid}])

    def delete_user(self, filters, params, fields):
        return self.delete_object(filters)

    def delete_slice(self, filters, params, fields):
        return self.delete_object(filters)

    def delete_authority(self, filters, params, fields):
        return self.delete_object(filters)

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

    @defer.inlineCallbacks
    def get_lease(self,filters,params,fields):
        result = yield self.get_resource_lease(filters,fields,params)
        defer.returnValue(result['lease'])

    # This get_resource is about the AM only... let's forget about RM for the time being
    @defer.inlineCallbacks
    def get_resource(self, filters, params, fields):
        result = yield self.get_resource_lease(filters, fields, params)
        defer.returnValue(result['resource'])

    @defer.inlineCallbacks
    def get_resource_lease(self, filters, params, fields, list_resources = True, list_leases = True):
        if self.user['email'] in DEMO_HOOKS:
            rspec = open('/usr/share/manifold/scripts/nitos.rspec', 'r')
            defer.returnValue(self.parse_sfa_rspec(rspec))
            return 


        # Do we have a way to find slices, for now we only support explicit slice names
        # Note that we will have to inject the slice name into the resource object if not done by the parsing.
        # slice - resource is a NxN relationship, not well managed so far

        # ROUTERV2
        slice_urns = make_list(filters.get_op('slice', (eq, included)))
        slice_urn = slice_urns[0] if slice_urns else None
        slice_hrn, _ = urn_to_hrn(slice_urn) if slice_urn else (None, None)
        # XXX ONLY ONE AND WITHOUT JOKERS

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

        # Manage Rspec versions
        if 'rspec_type' and 'rspec_version' in self.config:
            api_options['geni_rspec_version'] = {'type': self.config['rspec_type'], 'version': self.config['rspec_version']}
        else:
            # For now, lets use GENIv3 as default
            api_options['geni_rspec_version'] = {'type': 'GENI', 'version': '3'}
            #api_options['geni_rspec_version'] = {'type': 'SFA', 'version': '1'}  
 
        if slice_hrn:
            cred = self._get_cred('slice', slice_hrn, v3 = self.am_version['geni_api'] != 2)
            api_options['geni_slice_urn'] = slice_urn
        else:
            cred = self._get_cred('user', v3= self.am_version['geni_api'] != 2)

        # Due to a bug in sfawrap, we need to disable caching on the testbed
        # side, otherwise we might not get RSpecs without leases
        # Anyways, caching on the testbed side is not needed since we have more
        # efficient caching on the client side
        api_options['cached'] = False

        if list_resources:
            if list_leases:
                api_options['list_leases'] = 'all' 
            else:
                api_options['list_leases'] = 'resources'
        else:
            if list_leases:
                api_options['list_leases'] = 'leases'
            else:
                raise Exception, "Neither resources nor leases requested in ListResources"
            
        if self.am_version['geni_api'] == 2:
            # AM API v2 
            result = yield self.sliceapi.ListResources([cred], api_options)
        else:
            # AM API v3
            if slice_hrn:
                result = yield self.sliceapi.Describe([slice_urn], [cred], api_options)
                result['value'] = result['value']['geni_rspec']
            else:
                result = yield self.sliceapi.ListResources([cred], api_options)
                
        if not 'value' in result or not result['value']:
            raise Exception, result['output']

        rspec_string = result['value']
        rsrc_slice = self.parse_sfa_rspec(rspec_string)
  

        if slice_urn:
            for r in rsrc_slice['resource']:
                # XXX We might consider making this a list...
                r['slice'] = slice_urn

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

    def __init__(self, router, platform, query, config, user_config, user):
#        FromNode.__init__(self, platform, query, config)
        super(SFAGateway, self).__init__(router, platform, query, config, user_config, user)
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
        return "<SFAGateway %r: %s>" % (self.config['sm'], self.query)

    @defer.inlineCallbacks
    def start(self):
        super(SFAGateway, self).start()
        try:
            assert self.query, "Cannot run gateway with not query associated: %s" % self.platform
            q = self.query

            self.debug = 'debug' in self.query.params and self.query.params['debug']

            yield self.bootstrap()

            if not self.user_config:
                self.send(LastRecord())
                return

            fields = q.fields # Metadata.expand_output_fields(q.object, list(q.fields))
            Log.debug("SFA CALL START %s_%s" % (q.action, q.object), q.filters, q.params, fields)
            records = yield getattr(self, "%s_%s" % (q.action, q.object))(q.filters, q.params, fields)

            if q.object in self.map_fields:
                Rename(self, self.map_fields[q.object])
            
            # Return result
            map(self.send, Records(records))
            self.send(LastRecord())

        except Exception, e:
            Log.error(" contacting %s Query (action = %s , object = %s, filters = %s, fields = %s, params = %s)" % (self.platform,q.action,q.object,q.filters,q.fields,q.params))
            traceback.print_exc()
            rv = ResultValue(
                origin      = (ResultValue.GATEWAY, self.__class__.__name__, self.platform, str(self.query)),
                type        = ResultValue.ERROR, 
                code        = ResultValue.ERROR, 
                description = str(e), 
                traceback   = traceback.format_exc())
            self.result_value.append(rv)

            # return None to inform that everything has been transmitted
            self.send(LastRecord())


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
        if isinstance(platform, Platform):
            platform = platform.platform           
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
        is_admin = self.is_admin(user)

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
        # Why do we need slice credentials for admin user???
        #need_slice_credentials = is_admin or need_delegated_slice_credentials
        need_slice_list = need_slice_credentials
        # Why do we need authority credentials for admin user???
        #need_authority_credentials = is_admin or need_delegated_authority_credentials
        need_authority_credentials = need_delegated_authority_credentials
        need_authority_list = need_authority_credentials
        need_delegated_user_credential = not is_admin and self.credentials_needed('delegated_user_credential', config)
        if need_slice_list:
            pass
            #Log.tmp('is admin = need slice credentials')
            #Log.tmp('need slice list')
        need_gid = not 'gid' in config
        need_user_credential = is_admin or need_authority_credentials or need_slice_list or need_slice_credentials or need_delegated_user_credential or need_gid

        if self.is_admin(self.user):
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
        registry_proxy = self.make_user_proxy(self.config['registry'], config, 'sscert')
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
            # In case the user is PI on several authorities
            # fix to be applied in ROUTERV2
            try:
                config['authority_list'] = record['reg-pi-authorities']
            except Exception:
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
