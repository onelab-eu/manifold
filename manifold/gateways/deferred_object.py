#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Virtual pure class that must be implemented by deferred objects 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                          import GeneratorType
from twisted.internet               import defer

from manifold.util.type           	import accepts, returns 

class DeferredObject(object):
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
            A dictionnary {String : String} mapping a Gateway field name
            with the corresponding MANIFOLD field name.
        """
        return cls.aliases

    #@returns(Gateway)
    def get_gateway(self):
        """
        Returns:
            The Gateway instance related to this Object.
        """
        return self.gateway

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def create(self, query, annotation):
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of updated Objects.
        """
        raise NotImplementedError("%s::create method is not implemented" % self.__class__.__name__)

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def delete(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of updated Objects.
        """
        raise NotImplementedError("%s::delete method is not implemented" % self.__class__.__name__)

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def update(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of updated Objects.
        """
        raise NotImplementedError("%s::update method is not implemented" % self.__class__.__name__)

    @defer.inlineCallbacks
    @returns(GeneratorType)
    def get(self, query, annotation): 
        """
        Retrieve an Object from the Gateway.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A dictionnary containing the requested object.
        """
        raise NotImplementedError("Not implemented")

    @returns(list)
    def make_announces(self):
        """
        Returns:
            The list of Announce instances related to this object.
        """
        raise NotImplementedError("Not implemented")

