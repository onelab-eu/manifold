#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# Slice object exposed by a RM. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                                      import GeneratorType
from xmlrpclib                                  import DateTime
from twisted.internet                           import defer

from sfa.util.xrn                               import hrn_to_urn 

from manifold.core.record                       import Record
from manifold.gateways.deferred_object          import DeferredObject
from manifold.util.log                          import Log
from manifold.util.predicate                    import eq, lt, le, included
from manifold.util.type                         import accepts, returns
from manifold.util.misc                         import make_list

# XXX Clarify what is a deferred object. It's unlikely we need something like this
class RM_Object(DeferredObject):

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get(self, user, account_config, query):
        """
        Retrieve an RM_Object from SFA.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            A dictionnary containing the requested SFA object.
            Ex: for an User, the dictionnary contains:
                role_ids: list of integer
                last_updated: an integer (timestamp)
                slices: list of String (slice names)
                authority: a String (authority name)
                reg-urn: a String containing the object URN (ex: "urn:publicid:IDN+ple:upmc+user+john_doe")
                [...]
                enabled: a boolean
                geni_urn: a String containing the GENI URN (ex: "urn:publicid:IDN+ple:upmc+user+john_doe")
                date_created: an integer (timestamp)
        """
        object     = query.get_from()
        object_hrn = "%s_hrn" % object
        filters    = query.get_where()
        params     = query.get_params()
        fields     = query.get_select()
        gateway    = self.get_gateway()

        #-----------------------------------------------------------------------
        # Connection
        #   . Authentication : admin account thanks to sfa_proxy
        #   . Authorization  : delegated credentials by user
        # XXX Other schemes to be taken into account
        
        sfa_proxy = yield gateway.get_sfa_proxy_admin()
        # We need to do the same as is done for AM crendetials
        credential = gateway.get_credential(user, 'user', None)

        #-----------------------------------------------------------------------
        # Optimization of results

        # Let's find some additional information in filters in order to restrict our research
        # SFA gateways are not supporting Selection operation, however, we can optimize
        # the SFA query if it involves filters restricting the query to a given authority etc.
        authority_hrns = make_list(filters.get_op("authority_hrn", [eq, lt, le])) # TODO this should be specific to Authority object ?
        object_hrns    = make_list(filters.get_op(object_hrn,      [eq, included]))

        # Retrieve interface HRN (ex: "ple")
        interface_hrn = yield gateway.get_hrn()
        # Recursive: Should be based on jokers, eg. ple.upmc.*
        # Resolve  : True: make resolve instead of list
        if object_hrns:
            print "We have objects hrns", object_hrns
            
            # 0) given object name

            # If the objects are not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            object_hrns = [object_hrn for object_hrn in object_hrns if object_hrn.startswith(interface_hrn)]
            if not object_hrns:
                defer.returnValue(list())

            # Check for jokers ?
            # 'recursive' won't be evaluated in the rest of the function
            stack   = object_hrns
            resolve = True

        elif authority_hrns:
            # 2) given authority

            # If the authority is not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            Log.warning("calling startswith on a list ??")
            authority_hrns = [a for a in authority_hrns if a.startswith(interface_hrn)]
            if not authority_hrns:
                defer.returnValue(list())

            resolve   = False
            recursive = False
            stack     = list() 

            ####################
            # TODO
            # This is a check since the next loop is crappy and has a non-deterministic result
            # for instance if:
            #   authority_hrns = ["ple.upmc.foo", "ple.*"]
            #   authority_hrns = ["ple.*", "ple.upmc.foo"]
            # Moreover it does not handle properly list like:
            #   authority_hrns = ["ple.a.*", ple.b.*"] since the result will be ["ple"]
            if len(authority_hrns) > 1:
                num_stars = 0
                no_star = False
                for authority_hrn in authority_hrns:
                    if '*' in authority_hrn: num_stars += 1 
                    else:                    no_star    = True
                if num_stars > 1 or (num_stars > 0 and no_star):
                    Log.warning("get_object: the next loop may have a non-deterministic result")
            ####################

            for authority_hrn in authority_hrns:
                if not '*' in authority_hrn: # jokers ?
                    stack.append(authority_hrn)
                else:
                    stack = [interface_hrn]
                    break

        else: # Nothing given
            resolve   = False
            recursive = True
            stack     = [interface_hrn]
        
        # TODO: user's objects, use reg-researcher

        #-----------------------------------------------------------------------
        # Result construction

        output = list() 

        if resolve:
            # stack = ['ple.upmc.loic_baron'] --> ['urn:publicid:IDN+ple:upmc+user+loic_baron']
            stack = map(lambda x: hrn_to_urn(x, object), stack)
            _result, = yield sfa_proxy.Resolve(stack, credential, {'details': True})

            # XXX How to better handle DateTime XMLRPC types into the answer ?
            # XXX Shall we type the results like we do in CSV ?
            result = dict()
            for k, v in _result.items():
                if isinstance(v, DateTime):
                    result[k] = str(v) # datetime.strptime(str(v), "%Y%m%dT%H:%M:%S") 
                else:
                    result[k] = v

            output.append(result)
        else:
            # Build a list of deferred tasks (one per queried SFA object)
            deferreds = list() 
            while stack:
                auth_xrn = stack.pop()
                deferred = sfa_proxy.List(auth_xrn, credential, {'recursive': recursive})
                deferreds.append(deferred)
                    
            result = yield defer.DeferredList(deferreds)

            for (success, records) in result:
                if not success:
                    continue
                output.extend([r for r in records if r['type'] == object])

        defer.returnValue(output)

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def create(self, user, account_config, query):
        filter = query.get_filter()
        params = query.get_params()
        fields = query.get_fields()

        # Create a reversed map : MANIFOLD -> SFA
        rmap = { v: k for k, v in self.map_user_fields.items() }

        new_params = dict()
        for key, value in params.items():
            if key in rmap:
                new_params[rmap[key]] = value
            else:
                new_params[key] = value

        # XXX should call create_record_from_new_params which would rely on mappings
        dict_filters = filter.to_dict()
        if self.query.object + '_hrn' in new_params:
            object_hrn = new_params[self.query.object+'_hrn']
        else:
            object_hrn = new_params['hrn']
        if 'hrn' not in new_params:
            new_params['hrn'] = object_hrn
        if 'type' not in new_params:
            new_params['type'] = self.query.object
            #raise Exception, "Missing type in new_params"
        object_auth_hrn = get_authority(object_hrn)

        server_version = yield self.get_cached_server_version(self.registry)
        server_auth_hrn = server_version['hrn']

        if not new_params['hrn'].startswith('%s.' % server_auth_hrn):
            # XXX not a success, neither a warning !!
            print "I: Not requesting object creation on %s for %s" % (server_auth_hrn, new_params['hrn'])
            defer.returnValue([])

        auth_cred = self._get_cred('authority', object_auth_hrn)

        if 'type' not in new_params:
            raise Exception, "Missing type in new_params"
        try:
            object_gid = yield self.registry.Register(new_params, auth_cred)
        except Exception, e:
            raise Exception, 'Failed to create object: record possibly already exists: %s' % e
        defer.returnValue([{'hrn': new_params['hrn'], 'gid': object_gid}])

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def update(self, user, account_config, query):
        filter = query.get_filter()
        params = query.get_params()
        fields = query.get_fields()

        # XXX should call create_record_from_params which would rely on mappings
        dict_filters = filter.to_dict()
        if filter.has(self.query.object+'_hrn'):
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

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def delete(self, user, account_config, query):
        filter = query.get_filter()

        dict_filters = filter.to_dict()
        if filter.has(self.query.object+'_hrn'):
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

