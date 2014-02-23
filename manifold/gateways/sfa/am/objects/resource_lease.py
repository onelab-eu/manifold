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

class ResourceLease(DeferredObject):
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
        is_debug = "debug" in params and params["debug"]

        #-----------------------------------------------------------------------
        # Parameters
        #
        # Currently, we need the slice name to be explicitely given
        # We only keep the first one

        slice_hrns = make_list(filters.get_op("slice", (eq, included)))
        slice_hrn = slice_hrns[0] if slice_hrns else None

        #-----------------------------------------------------------------------
        # Connection

        rm_name    = gateway.get_first_rm_name()
        rm_gateway = gateway.get_interface().get_gateway(rm_name)

        # We create an SFA proxy with the authentication tokens from the RM
        # XXX In general we need to be sure that all we pass is trusted, whether
        # it is GID or credentials
        sfa_proxy = gateway.get_sfa_proxy_admin(rm_name)
        if slice_hrn:
            cred = rm_gateway.get_credential(user, "slice", slice_hrn)
        else:
            cred = rm_gateway.get_credential(user, "user")

        server_version = yield sfa_proxy.get_cached_version()
        am_api_version = server_version.get('geni_api', AM_API_v3)

        #-----------------------------------------------------------------------
        # Request : api_options

        # no need to check if server accepts the options argument since the options has
        # been a required argument since v1 API
        api_options = dict() 
        # always send call_id to v2 servers
        api_options["call_id"] = unique_call_id()
        # ask for cached value if available
        api_options["cached"] = True


        # XXX Cache might cause problems for leases
        # XXX Include selective choice of resources or leases

        #-----------------------------------------------------------------------
        # Request : RSpecs preference
        # 
        # We would need to implement some sort of autonegociation instead of
        # setting a default value

        rspec_type, rspec_version, rspec_version_string = gateway.get_rspec_version()
        api_options["geni_rspec_version"] = {
            "type"    : rspec_type,
            "version" : rspec_version
        }  
 
        if slice_hrn:
            api_options["geni_slice_urn"] = hrn_to_urn(slice_hrn, "slice")

        # Retrieve "advertisement" rspec
        # We guess the AM API version from the requested version, that is wrong # !!
        if am_api_version == 2:
            # AM API v2 
            result = yield sfa_proxy.ListResources([cred], api_options)
        else:
            # AM API v3
            if slice_hrn:
                slice_urn = api_options["geni_slice_urn"]
                result = yield sfa_proxy.Describe([slice_urn], [cred], api_options)
                result["value"] = result["value"]["geni_rspec"]
            else:
                result = yield sfa_proxy.ListResources([cred], api_options)
                
        if not "value" in result or not result["value"]:
            raise Exception, result["output"]

        rspec_string = result["value"]
        rsrc_slice = gateway.parse_sfa_rspec(rspec_version_string, rspec_string)

        if slice_hrn:
            for r in rsrc_slice["resource"]:
                r["slice"] = slice_hrn

        if is_debug:
            rsrc_slice["debug"] = {"rspec" : rspec}
        defer.returnValue(rsrc_slice)
