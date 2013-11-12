#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class gathers common methods exposed by a SFA-RM.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                                      import GeneratorType
from twisted.internet                           import defer

from manifold.util.type                 	import accepts, returns 

class Object:
    aliases = dict()

    def __init__(self, gateway):
        """
        Constructor
        """
        self.gateway = gateway

    @classmethod
    @returns(dict)
    def get_aliases(cls):
        """
        Returns:
            A dictionnary {String : String} mapping a SFA field name
            with the corresponding MANIFOLD field name.
        """
        return cls.aliases

    #@returns(SFAGatewayCommon)
    def get_gateway(self):
        """
        Returns:
            The SFAGatewayCommon instance related to this Object.
        """
        return self.gateway

    @defer.inlineCallbacks
    @returns(GeneratorType)
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
        raise Exception, "%s::create method is not implemented" % self.__class__.__name__

    @defer.inlineCallbacks
    @returns(GeneratorType)
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
        raise Exception, "%s::delete method is not implemented" % self.__class__.__name__

    @defer.inlineCallbacks
    @returns(GeneratorType)
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
        raise Exception, "%s::update method is not implemented" % self.__class__.__name__

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get(self, user, account_config, query): 
        """
        Retrieve an Object from SFA.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            A dictionnary containing the requested SFA object.
        """
        raise Exception, "Not implemented"
