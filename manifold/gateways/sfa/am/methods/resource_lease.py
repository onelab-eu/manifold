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

from types                                      import StringTypes, GeneratorType
from twisted.internet                           import defer

class ResourceLease(Object):
    @defer.inlineCallbacks
    def get(self, user, user_account_config, query):
    #def get_resource_lease(self, user, user_config, filters, params, fields):
        """
        (Internal use)
        Args:
            user: A dictionnary describing the User performing the Query.
            user_account_config: A dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        """
        gateway  = self.get_gateway()
        fields   = query.get_select()
        filters  = query.get_where()
        params   = query.get_params()
        is_debug = "debug" in params and params["debug"]:

#MANDO|        if self.user.email in DEMO_HOOKS:
#MANDO|            rspec = open('/usr/share/manifold/scripts/nitos.rspec', 'r')
#MANDO|            defer.returnValue(self.parse_sfa_rspec(rspec))
#MANDO|            return 

        # Do we have a way to find slices, for now we only support explicit slice names
        # Note that we will have to inject the slice name into the resource object if not done by the parsing.
        # slice - resource is a NxN relationship, not well managed so far

        slice_hrns = make_list(filters.get_op("slice", (eq, included)))
        # XXX ONLY ONE AND WITHOUT JOKERS
        slice_hrn = slice_hrns[0] if slice_hrns else None

        slice_api = self.get_sfa_proxy()
        # no need to check if server accepts the options argument since the options has
        # been a required argument since v1 API
        api_options = dict() 

        # always send call_id to v2 servers
        api_options["call_id"] = unique_call_id()

        # ask for cached value if available
        api_options["cached"] = True

        # Get server capabilities
        server_version = gateway.get_cached_server_version(slice_api)
        type_version = set()

        # Manage Rspec versions
        if "rspec_type" and "rspec_version" in platform_config:
            api_options["geni_rspec_version"] = {
                "type"    : platform_config["rspec_type"],
                "version" : platform_config["rspec_version"]
            }
        else:
            # For now, lets use SFAv1 as default
            api_options["geni_rspec_version"] = {
                "type"    : "SFA",
                "version" : "1"
            }  
 
        if slice_hrn:
            cred = gateway.get_credential(user, user_account_config, "slice", slice_hrn)
            api_options["geni_slice_urn"] = hrn_to_urn(slice_hrn, "slice")
        else:
            cred = gateway.get_credential(user, user_account_config, "user")

        # Retrieve "advertisement" rspec
        if self.version["geni_api"] == 2:
            # AM API v2 
            result = yield slice_api.ListResources([cred], api_options)
        else:
            # AM API v3
            if slice_hrn:
               slice_urn = api_options["geni_slice_urn"]
               result = yield slice_api.Describe([slice_urn], [cred], api_options)
               result["value"] = result["value"]["geni_rspec"]
            else:
               result = yield slice_api.ListResources([cred], api_options)
                
        if not "value" in result or not result["value"]:
            raise Exception, result["output"]

        rspec_string = result["value"]
        rsrc_slice = self.parse_sfa_rspec(rspec_string)

        if slice_hrn:
            for r in rsrc_slice["resource"]:
                r["slice"] = slice_hrn

        if is_debug:
            rsrc_slice["debug"] = {"rspec" : rspec}
        defer.returnValue(rsrc_slice)
