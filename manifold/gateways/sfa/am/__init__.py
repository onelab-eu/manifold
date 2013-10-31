#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to query a SFA Aggregate Manager.
# http://www.opensfa.info/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

import traceback
from types                              import GeneratorType, StringTypes, ListType
from twisted.internet                   import defer

from sfa.rspecs.rspec                   import RSpec
from sfa.util.xrn                       import hrn_to_urn, urn_to_hrn

from manifold.core.filter               import Filter
from manifold.gateways.sfa              import SFAGatewayCommon, DEMO_HOOKS
from manifold.gateways.sfa.user         import ADMIN_USER, check_user  
from manifold.gateways.sfa.proxy        import SFAProxy
from manifold.gateways.sfa.rspecs.SFAv1 import SFAv1Parser # as Parser
from manifold.util.log                  import Log
from manifold.util.misc                 import make_list
from manifold.util.predicate            import contains, eq, lt, le, included
from manifold.util.type                 import accepts, returns 

class SFA_AMGateway(SFAGatewayCommon):
    def __init__(self, interface, platform, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the platform_configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        super(SFA_AMGateway, self).__init__(interface, platform, platform_config)
        platform_config = self.get_config()

        if not "sm" in platform_config:
            raise KeyError("'sm' is missing in platform_configuration: %s (%s)" % (platform_config, type(platform_config)))

    @returns(GeneratorType)
    def get_rms(self):
        """
        Returns:
            Allow to iterate on each Platform corresponding to each RM of this AM.
        """
        platform_names = self.get_config()["rm_platforms"].all()
        assert len(platform_names) > 1, "This AM does not refer to a RM!"
        for platform_name in platform_names:
            platform = db.query(Platform).filter(Platform.gateway_type == "sfa_rm").filter(Platform.platform == platform_name)
            assert isinstance(platform, Platform), "Invalid platform = %s (%s)" % (platform, type(platform))
            yield platform

    @returns(StringTypes)
    def get_url(self):
        """
        Returns:
            A String instance containing the URL of the Aggregate
            Manager managed by this Gateway. 
        """
        return self.get_config()["sm"]

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return "<%s %r>" % (self.__class__.__name__, self.get_url())

    @staticmethod
    @returns(StringTypes)
    def build_sfa_rspec(slice_urn, resources, leases):
        parser = SFAv1Parser(resources, leases)
        return parser.to_rspec(slice_urn)

    @defer.inlineCallbacks
    def update_slice(self, user, user_config, filters, params, fields):
        if 'resource' not in params:
            raise Exception, "Update failed: nothing to update"

        # Keys
        if not filters.has_eq('slice_hrn'):
            raise Exception, 'Missing parameter: slice_hrn'

        slice_hrn = filters.get_eq('slice_hrn')
        slice_urn = hrn_to_urn(slice_hrn, 'slice')
        resources = params['resource'] if 'resource' in params else list()
        leases    = params['lease']    if 'lease'    in params else list()

        # Credentials
        user_cred  = self._get_cred(user, user_config, 'user')
        slice_cred = self._get_cred(user, user_config, 'slice', slice_hrn)

        # Build a rspec "request" gathering requested resources (ideally we should send
        # to each testbed a rspec containing only its own resources) 
        rspec = SFA_AMGateway.build_sfa_rspec(slice_urn, resources, leases)
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
        slice_records = yield self.registry.Resolve(slice_urn, [user_cred]) # <<< TODO this should be retrieve by querying the Router pointed by this Gateway (see self.interface)
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
            # TODO strange <<<
            server_version = yield self.get_cached_server_version(self.registry)
            if 'sfa' not in server_version:
                print "W: converting to pg rspec"
                users = pg_users_arg(user_records)
                rspec = RSpec(rspec)
                rspec.filter({'component_manager_id': server_version['urn']})
                rspec = RSpecConverter.to_pg_rspec(rspec.toxml(), content_type='request')
            else:
                users = sfa_users_arg(user_records, slice_record)
            # >>> strange
        # do not append users, keys, or slice tags. Anything
        # not contained in this request will be removed from the slice

        # CreateSliver has supported the options argument for a while now so it should
        # be safe to assume this server support it
        slice_api = self.get_server()
        api_options = dict() 
        api_options ['append'] = False
        api_options ['call_id'] = unique_call_id()
        ois = yield self.ois(slice_api, api_options)

        version = self.get_cached_server_version(slice_api)
        if version["geni_api"] == 2:
#MANDO|        if self.version['geni_api'] == 2:
            # AM API v2
            result = yield slice_api.CreateSliver(slice_urn, [slice_cred], rspec, users, ois)
        else:
            # AM API v3
            result = yield slice_api.Allocate(slice_urn, [slice_cred], rspec, ois) # TODO we do not pass users ?
            result = yield slice_api.Provision([slice_urn], [slice_cred], ois)     # TODO we do not pass users ?

        # Manifest is a rspec file summarizing what we got.
        manifest = ReturnValue.get_value(result)

        if not manifest:
            Log.error("No manifest from %s: %r" % (self.get_platform_name(), result))
            defer.returnValue(list())
            # TODO self.error(...)
        else:
            Log.info("Got manifest from %s" % self.get_platform_name())
        rsrc_leases = self.parse_sfa_rspec(manifest)

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        #print "oK"
        #print "SLICE=", slice
        defer.returnValue([slice])

    def get_slice_demo(self, user, user_config, filters, params, fields):
            Log.warning("Demo hook")
            s= {}
            s['slice_hrn'] = "ple.upmc.agent"
            s['slice_description'] = 'DEMO SLICE'

            if self.get_platform_name() != 'ple':
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
            defer.returnValue(self.parse_sfa_rspec(rspec))
            return 

        # Do we have a way to find slices, for now we only support explicit slice names
        # Note that we will have to inject the slice name into the resource object if not done by the parsing.
        # slice - resource is a NxN relationship, not well managed so far

        slice_hrns = make_list(filters.get_op('slice', (eq, included)))
        # XXX ONLY ONE AND WITHOUT JOKERS
        slice_hrn = slice_hrns[0] if slice_hrns else None

        slice_api = self.get_server()
        # no need to check if server accepts the options argument since the options has
        # been a required argument since v1 API
        api_options = {}
        # always send call_id to v2 servers
        api_options ['call_id'] = unique_call_id()
        # ask for cached value if available
        api_options ['cached'] = True
        # Get server capabilities
        server_version = yield self.get_cached_server_version(slice_api)
        type_version = set()

        # Manage Rspec versions
        if 'rspec_type' and 'rspec_version' in self.config:
            api_options['geni_rspec_version'] = {'type': self.config['rspec_type'], 'version': self.config['rspec_version']}
        else:
            # For now, lets use SFAv1 as default
            api_options['geni_rspec_version'] = {'type': 'SFA', 'version': '1'}  
 
        if slice_hrn:
            cred = self._get_cred(user, user_config, 'slice', slice_hrn)
            api_options['geni_slice_urn'] = hrn_to_urn(slice_hrn, 'slice')
        else:
            cred = self._get_cred(user, user_config, 'user')

        # Retrieve "advertisement" rspec
        if self.version['geni_api'] == 2:
            # AM API v2 
            result = yield slice_api.ListResources([cred], api_options)
        else:
            # AM API v3
            if slice_hrn:
               slice_urn = api_options['geni_slice_urn']
               result = yield slice_api.Describe([slice_urn], [cred], api_options)
               result['value'] = result['value']['geni_rspec']
            else:
               result = yield slice_api.ListResources([cred], api_options)
                
        if not 'value' in result or not result['value']:
            raise Exception, result['output']

        rspec_string = result['value']
        rsrc_slice = self.parse_sfa_rspec(rspec_string)

        if slice_hrn:
            for r in rsrc_slice['resource']:
                r['slice'] = slice_hrn

        if self.debug:
            rsrc_slice['debug'] = {'rspec': rspec}
        defer.returnValue(rsrc_slice)

    # JORDAN am
    def add_rspec_to_cache(self, slice_hrn, rspec):
        Log.warning("RSpec caching disabled")
        return
        # Cache result (XXX bug CreateSliver / need to invalidate former cache entries ?)
        # We might need to update a cached entry when modified instead of creating a new one
        rspec_add = {
            "rspec_person_id"  : self.get_config()["caller"]["person_id"],
            "rspec_target"     : slice_hrn,
            "rspec_hash"       : hashlib.md5(rspec).hexdigest(),
            #"rspec_expiration" : XXX
            "rspec"            : rspec
        }
        new = MySliceRSpec(self.api, rspec_add)
        new.sync()
        if not new['rspec_id'] > 0:
            # WARNING: caching failed
            pass

    ############################################################################ 
    # RSPEC PARSING
    ############################################################################ 

    @returns(dict)
    def parse_sfa_rspec(self, rspec_string):
        # rspec_type and rspec_version should be set in the config of the platform,
        # we use GENIv3 as default one if not
        if 'rspec_type' and 'rspec_version' in self.config:
            rspec_version = self.config['rspec_type'] + ' ' + self.config['rspec_version']
        else:
            rspec_version = 'SFA 1'

        rspec = RSpec(rspec_string, version=rspec_version)
        
        nodes = rspec.version.get_nodes()
        leases = rspec.version.get_leases()
        #channels = rspec.version.get_channels()
 
        # Extend object and Format object field's name
        for node in nodes:
             node['hrn'] = urn_to_hrn(node['component_id'])[0]
             node['urn'] = node['component_id']
             node['hostname'] = node['component_name']
             node['initscripts'] = node.pop('pl_initscripts')
        
        return {'resource': nodes,'lease': leases } 
#               'channel': channels \
#               }

    @defer.inlineCallbacks
    @returns(list)
    def ois(self, server, option_dict):
        """
        ois = options if supported
        to be used in something like serverproxy.Method (arg1, arg2, *self.ois(api_options))
        Args:
            server:
            option_dict
        """
        flag = yield self.server_supports_options_arg(server)
        if flag:
            defer.returnValue(option_dict)
        else:
            flag = yield self.server_supports_call_id_arg(server)
            if flag:
                defer.returnValue([unique_call_id()])
            else:
                defer.returnValue([])

    #--------------------------------------------------------------------------
    # Server 
    #--------------------------------------------------------------------------

    ### resurrect this temporarily so we can support V1 aggregates for a while
    # MANDO move in SFAProxy
    @defer.inlineCallbacks
    @returns(bool)
    def server_supports_options_arg(self, server):
        """
        Args:
            server: A SFAProxy instance.
        Returns:
            True iif servers supports ??? 
        """
        server_version = yield self.get_cached_server_version(server)
        # XXX need to rewrite this 
        # XXX added not server version to handle cases where GetVersion fails (jordan)
        if not server_version or int(server_version.get('geni_api')) >= 2:
            defer.returnValue(True)
            return 
        defer.returnValue(False)
        
    # MANDO move in SFAProxy
    @defer.inlineCallbacks
    @returns(bool)
    def server_supports_call_id_arg(self, server):
        """
        Args:
            server: A Proxy instance  
        Returns:
            True iif the server supports the optional "call_id" arg.
        """
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


