#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Virtual pure class that must be implemented by non-deferred objects 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from types                          import GeneratorType

from manifold.core.annotation       import Annotation
from manifold.core.announce         import Announce
from manifold.core.query            import Query
from manifold.util.type           	import accepts, returns 

class Object(object):
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

    def check(self, query, annotation):
        assert isinstance(query, Query),\
            "Invalid query = %s (%s)" % (query, type(query))
        assert not annotation or isinstance(annotation, Annotation),\
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))

    @returns(GeneratorType)
    def create(self, query, annotation):
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A GeneratorType over a Records instance (created Records) 
        """
        self.check(query, annotation)
        raise NotImplementedError("%s::create method is not implemented" % self.__class__.__name__)

    @returns(GeneratorType)
    def update(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A GeneratorType over a Records instance (updated Records) 
        """
        self.check(query, annotation)
        raise NotImplementedError("%s::update method is not implemented" % self.__class__.__name__)

    @returns(GeneratorType)
    def delete(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A GeneratorType over a Records instance (deleted Records) 
        """
        self.check(query, annotation)
        raise NotImplementedError("%s::delete method is not implemented" % self.__class__.__name__)

    @returns(GeneratorType)
    def get(self, query, annotation): 
        """
        Retrieve an Object from the Gateway.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A GeneratorType over a Records instance (fetched Records) 
        """
        self.check(query, annotation)
        raise NotImplementedError("Not implemented")

    @returns(Announce)
    def make_announce(self):
        """
        Returns:
            The Announce related to this object.
        """
        raise NotImplementedError("Not implemented")

