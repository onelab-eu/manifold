#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class gathers common methods exposed by a SFA-RM.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                                  import StringTypes, GeneratorType
from twisted.internet                       import defer
from xmlrpclib                              import DateTime
from sfa.util.xrn                           import hrn_to_urn 

from manifold.util.log                      import Log
from manifold.util.predicate                import eq, lt, le, included
from manifold.util.type                     import accepts, returns
from manifold.util.misc                     import make_list

class Object:
    aliases = dict()

    def __init__(self, gateway):
        """
        Constructor
        """
        self.gateway = gateway

#NOT_YET_IMPLEMENTED|
#NOT_YET_IMPLEMENTED|    @staticmethod
#NOT_YET_IMPLEMENTED|    @returns(StringTypes)
#NOT_YET_IMPLEMENTED|    def get_alias(field_name):
#NOT_YET_IMPLEMENTED|        """
#NOT_YET_IMPLEMENTED|        Args:
#NOT_YET_IMPLEMENTED|            field_name: The name of a field related to an SFA object
#NOT_YET_IMPLEMENTED|        """
#NOT_YET_IMPLEMENTED|        assert isinstance(field_name, StringTypes), "Invalid field name: %s (%s)" % (field_name, type(field_name))
#NOT_YET_IMPLEMENTED|        return Object.aliases[field_name]
#NOT_YET_IMPLEMENTED|

    #@returns(SFAGatewayCommon)
    def get_gateway(self):
        """
        Returns:
            The SFAGatewayCommon instance related to this Object.
        """
        return self.gateway

    @defer.inlineCallbacks
    @returns(list)
    def create(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            The list of updated Objects.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(list)
    def delete(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            The list of updated Objects.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(list)
    def update(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            The list of updated Objects.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get(self, user, account_config, query): 
        """
        (Internal use)
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
        Log.tmp("-" * 80)
        Log.tmp("user = %s" % user)
        Log.tmp(query)
        Log.tmp("-" * 80)

        object     = query.get_from()
        object_hrn = "%s_hrn" % object
        filters    = query.get_where()
        params     = query.get_params()
        fields     = query.get_select()
        gateway    = self.get_gateway()

        # Let's find some additional information in filters in order to restrict our research
        authority_hrns = make_list(filters.get_op("authority_hrn", [eq, lt, le])) # TODO this should be specific to Authority object ?
        object_hrns    = make_list(filters.get_op(object_hrn,      [eq, included]))
        if not object_hrns:
            raise Exception("The WHERE clause of the following Query does not allow to retrieve an HRN: %s" % query)

        # Retrieve interface HRN (ex: "ple")
        interface_hrn = yield gateway.get_hrn()

        # Recursive: Should be based on jokers, eg. ple.upmc.*
        # Resolve  : True: make resolve instead of list
        if object_hrns:
            # 0) given object name

            # If the objects are not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            object_hrns = [object_hrn for object_hrn in object_hrns if object_hrn.startswith(interface_hrn)]
            if not object_hrns:
                defer.returnValue(list())

            # Check for jokers ?
            # 'recursive' won't be evaluated in the rest of the function
            stack     = object_hrns
            resolve   = True

        elif authority_hrns:
            # 2) given authority

            # If the authority is not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            Log.warning("calling startswith on a list ??")
            if not authority_hrns.startswith(interface_hrn):
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
        
        cred = gateway._get_cred(user, account_config, "user", None)

        registry = yield gateway.get_server()
        if resolve:
            # stack = ['ple.upmc.loic_baron'] --> ['urn:publicid:IDN+ple:upmc+user+loic_baron']
            stack = map(lambda x: hrn_to_urn(x, object), stack)
            _result,  = yield registry.Resolve(stack, cred, {'details': True})

            # XXX How to better handle DateTime XMLRPC types into the answer ?
            # XXX Shall we type the results like we do in CSV ?
            result = dict()
            for k, v in _result.items():
                if isinstance(v, DateTime):
                    result[k] = str(v) # datetime.strptime(str(v), "%Y%m%dT%H:%M:%S") 
                else:
                    result[k] = v

            defer.returnValue([result])
        
        if len(stack) > 1:
            deferreds = list() 
            while stack:
                auth_xrn = stack.pop()
                deferred = registry.List(auth_xrn, cred, {'recursive': recursive})
                deferreds.append(deferred)
                    
            result = yield defer.DeferredList(deferreds)

            output = list() 
            for (success, records) in result:
                if not success:
                    continue
                output.extend([r for r in records if r['type'] == object])
            defer.returnValue(output)

        else:
            auth_xrn = stack.pop()
            records = yield registry.List(auth_xrn, cred, {'recursive': recursive})
            records = [r for r in records if r['type'] == object]
            defer.returnValue(records)


