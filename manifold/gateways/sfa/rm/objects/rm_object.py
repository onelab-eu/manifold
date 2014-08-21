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

from sfa.util.xrn                               import urn_to_hrn, hrn_to_urn 

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
        object     = query.get_table_name()
        object_hrn = "%s_hrn" % object
        filters    = query.get_where()
        params     = query.get_params()
        fields     = query.get_select()
        gateway    = self.get_gateway()

        print "FIELDS", fields

        #-----------------------------------------------------------------------
        # Connection
        #   . Authentication : admin account thanks to sfa_proxy
        #   . Authorization  : delegated credentials by user
        # XXX Other schemes to be taken into account
        
        sfa_proxy = yield gateway.get_sfa_proxy_admin()
        # We need to do the same as is done for AM crendetials
        credential = gateway.get_credential(user, 'user', None)

        # 1. The best case is when objects are given by name, which allows a
        # direct lookup.  We will accept both HRNs and URNs in filters.
        # object_hrn property is currently unused.
        # hrn and urn fields are in the sfa naming scheme.
        object_hrns = make_list(filters.get_op('hrn', [eq, included]))
        object_urns = make_list(filters.get_op('reg-urn', [eq, included]))
        for urn in object_urns:
            hrn, _ = hrn_to_urn(urn, object)
            object_hrns.append(hrn)
        # 2. Otherwise, we run a recursive search from the most precise known
        # authority.
        authority_hrns = make_list(filters.get_op('parent_authority', [eq, lt, le]))
        # 3. In the worst case, we search from the root authority, eg. 'ple'
        interface_hrn = yield gateway.get_hrn()

        # Based on cases 1, 2 or 3, we build the stack of objects to
        # List/Resolve.
        details   = True

        if object_hrns: # CASE 1
            # If the objects are not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            object_hrns = [ hrn for hrn in object_hrns if hrn.startswith(interface_hrn)]
            if not object_hrns:
                defer.returnValue(list())

            # Check for jokers ?
            stack   = object_hrns
            do_list = False

        elif authority_hrns: # CASE 2
            # If the authority is not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            authority_hrns  = [a for a in authority_hrns if a.startswith(interface_hrn)]
            if not auth_hrn:
                defer.returnValue(list())

            recursive = False # XXX We are supposing only two levels of hierarchy
            stack = list()

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

            for hrn in authority_hrns:
                if not '*' in hrn: # jokers ?
                    stack.append(hrn)
                else:
                    stack = [interface_hrn]
                    break
            do_list = True

        else: # CASE 3
            recursive = True #if object != 'authority' else False
            stack = [interface_hrn]
            do_list = True


        #-----------------------------------------------------------------------
        # Result construction

        # First we eventually perform the List...
        # Even if the stack is reduced to a single element, let's suppose it has
        # multiple... Let's loop on the authorities on which to make a list

        output = list()

        if do_list: 
            # The aim of List is the get the list of object urns
            object_urns = list()

            deferred_list = list()
            while stack:
                auth_hrn = stack.pop()
                d = sfa_proxy.List(auth_hrn, credential, {'recursive': recursive}) # XXX
                deferred_list.append(d)

                # Insert root authority (if needed, NOTE order is not preserved)
                if object == 'authority':
                    object_urns.append(hrn_to_urn(auth_hrn, object))

            result = yield defer.DeferredList(deferred_list)

            for (success, records) in result:
                if not success: # XXX
                    continue
                # We have a list of records with hrn/type.
                for record in records:
                    # We only keep records of the right type
                    # This is needed if we pass hrns, or because of bugs in
                    # SFAWrap (is it that a list of urns is not properly taken into
                    # account ?)
                    if record['type'] != object:
                        continue
                    object_urns.append(hrn_to_urn(record['hrn'], object))

        else:
            # We make the list of object_urns from the stack of hrns
            object_urns = [hrn_to_urn(hrn, object) for hrn in stack]

        print "OBJECT URNS", object_urns

        # ... then the Resolve
        #
        # We need to call Resolve if we ask more than hrn/urn/type
        do_resolve = bool(set(fields) - set(['reg-urn', 'hrn', 'type']))
        if do_resolve:
            print "resolve"
            
            records = yield sfa_proxy.Resolve(object_urns, credential, {'details': details})
            for _record in records:
                # XXX Due to a bug in SFA Wrap, we need to filter the type of object returned
                # If 2 different objects have the same hrn, the bug occurs
                # Ex: ple.upmc.agent (user) & ple.upmc.agent (slice)
                if _record['type'] != object:
                    continue

                record = dict()
                for k, v in _record.items():
                    if isinstance(v, DateTime):
                        record[k] = str(v) # datetime.strptime(str(v), "%Y%m%dT%H:%M:%S") 
                    else:
                        record[k] = v

                output.append(record)
        else:
            print "no resolve"
            # Build final record from URN
            for urn in object_urns:
                hrn, type = urn_to_hrn(urn)

                record = dict()
                if 'reg-urn' in fields:
                    record['reg-urn'] = urn
                if 'hrn' in fields:
                    record['hrn'] = hrn
                if 'type' in fields:
                    record['type'] = type
                output.append(record)

        print "output=", output

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

