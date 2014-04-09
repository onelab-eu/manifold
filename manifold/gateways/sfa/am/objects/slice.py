#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class wrap ResourceLease Object provided by SFA AM.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                              import StringTypes, GeneratorType
from twisted.internet                   import defer
from manifold.util.misc                 import make_list
from manifold.util.predicate            import included, eq
from manifold.gateways.deferred_object  import DeferredObject 
from ...am                              import unique_call_id

AM_API_v2 = 2
AM_API_v3 = 3

class Slice(DeferredObject):
    @defer.inlineCallbacks
    def update(self, user, user_account_config, query):
        """
        (Internal use)
        Args:
            user: A dictionnary describing the User performing the Query.
            user_account_config: A dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        """

        if not 'resource' in params and not 'lease' in params:
            raise Exception, "Update failed: nothing to update"

        # XXX We might need information for resources not provided in the update query, such as the resource granularity, which is required for nitos

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
        print "build rspec", resources, leases
        try:
            rspec = self.build_sfa_rspec(slice_urn, resources, leases)
        except Exception, e:
            print "EXCEPTION BUILDING RSPEC", e
            import traceback
            traceback.print_exc()
            rspec = ''
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

            manifest_rspec = ReturnValue.get_value(result)
        else:
            # AM API v3
            api_options['sfa_users'] = sfa_users
            api_options['geni_users'] = geni_users

            # http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Allocate
            result = yield self.sliceapi.Allocate(slice_urn, [slice_cred], rspec, api_options)

            # http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Provision
            result = yield self.sliceapi.Provision([slice_urn], [slice_cred], api_options)

            rspec_sliver_result = ReturnValue.get_value(result)

            # The returned manifest covers only newly provisioned slivers. Use Describe to get a manifest of all provisioned slivers.
            manifest_rspec = rspec_sliver_result.get('geni_rspec')

        if not manifest_rspec:
            print "NO MANIFEST FROM", self.platform, result
            defer.returnValue([])
        else:
            print "GOT MANIFEST FROM", self.platform
            print "MANIFEST=", manifest_rspec
            sys.stdout.flush()


        rsrc_leases = self.parse_sfa_rspec(manifest_rspec)

        slice = {'slice_hrn': filters.get_eq('slice_hrn')}
        slice.update(rsrc_leases)
        #print "oK"
        #print "SLICE=", slice
        defer.returnValue([slice])

