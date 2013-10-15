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

from manifold.util.log                  	import Log
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

    def get_gateway(self):
        """
        Returns:
            The SFAGateway related to this Object
        """
        return self.gateway

    @defer.inlineCallbacks
    @returns(list)
    def create(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(list)
    def delete(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(list)
    def update(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        """
        raise Exception, "Not implemented"

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get(self, user, account_config, query): 
        """
        (Internal use)
        """
        Log.tmp(">>>>>>> welcome in Object::get() :)")
        object      = query.get_from()
        object_hrn  = "%s_hrn" % object
        filters     = query.get_where()
        params      = query.get_params()
        fields      = query.get_select()
        gateway     = self.get_gateway()

        # Let's find some additional information in filters in order to restrict our research
        object_name = make_list(filters.get_op(object_hrn,      [eq, included]))
        auth_hrn    = make_list(filters.get_op('authority_hrn', [eq, lt, le]))

        Log.tmp("0)c)")
        interface_hrn = yield gateway.get_hrn()

        # recursive: Should be based on jokers, eg. ple.upmc.*
        # resolve  : True: make resolve instead of list
        Log.tmp("1) object_name = %s auth_hrn = %s" % (object_name, auth_hrn))
        if object_name:
            # 0) given object name

            # If the objects are not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            object_name = [ on for on in object_name if on.startswith(interface_hrn)]
            if not object_name:
                Log.tmp("<<<< returning []")
                defer.returnValue(list())

            # Check for jokers ?
            stack     = object_name
            resolve   = True

        elif auth_hrn:
            # 2) given authority

            # If the authority is not part of the hierarchy, let's return [] to
            # prevent the registry to forward results to another registry
            # XXX This should be ensured by partitions
            if not auth_hrn.startswith(interface_hrn):
                Log.tmp("<<<< returning []")
                defer.returnValue(list())

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
            recursive = True
            stack = [interface_hrn]
        
        Log.tmp("2) object_name = %s auth_hrn = %s" % (object_name, auth_hrn))
        # TODO: user's objects, use reg-researcher
        
        cred = gateway._get_cred(user, account_config, 'user')

        registry = yield gateway.get_server()
        if resolve:
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

            Log.tmp("<<<< returning %s" % [result])
            defer.returnValue([result])
        
        Log.tmp("3) len(stack) = %s" % len(stack)) 
        if len(stack) > 1:
            deferred_list = list() 
            while stack:
                auth_xrn = stack.pop()
                d = registry.List(auth_xrn, cred, {'recursive': recursive})
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
            records = yield registry.List(auth_xrn, cred, {'recursive': recursive})
            records = [r for r in records if r['type'] == object]
            Log.tmp("<<<< returning %s" % records)
            defer.returnValue(records)


