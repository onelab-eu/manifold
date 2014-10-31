#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Virtual pure class that must be implemented by non-deferred objects 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

import copy
from types                          import GeneratorType

from manifold.core.annotation       import Annotation
from manifold.core.announce         import Announce, Announces, announces_from_docstring
from manifold.core.method           import Method
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
from manifold.core.table            import Table
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.type             import accepts, returns 


class ManifoldCollection(set):

    def __init__(self, cls = None):
        # cls is either a ManifoldObject
        if cls:
            self._cls = cls
        elif self.__doc__:
            announce, = Announces.from_string(self.__doc__, None)
            self._cls = ManifoldObject.from_announce(announce)
        else:
            raise NotImplemented

    def get_gateway(self):
        return self._gateway

    def set_gateway(self, gateway):
        self._gateway = gateway

    def get_router(self):
        return self.get_gateway().get_router()

    def get(self, *args, **kwargs):
        pass

    def insert(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def sync(self):
        pass

    def get_object(self):
        return self._cls

    def copy(self):
        return copy.deepcopy(self)

class ManifoldLocalCollection(ManifoldCollection):

    def get(self, query = None): # filter = None, fields = None):
        ret = list()
        # XXX filter and fields
        # XXX How to preserve the object class ?
        for x in self:
            y = x.copy()
            y.__class__ = Record
            ret.append(y)
        if ret:
            ret[-1].set_last()
        else:
            ret.append(Record(last=True))
        return ret

    def insert(self, obj):
        self.add(obj)

        # XXX What is the return value for a CREATE
        rec = obj.copy()
        rec.set_last()
        ret = Records()
        ret.append(rec)
        return ret

    def remove(self):
        self.remove(obj)

class ManifoldRemoteCollection(ManifoldCollection):
    def sync(self):
        pass
    
class ManifoldObject(Record):

    __metaclass__   = PluginFactory
    __plugin__name__attribute__ = '__object_name__'

    __object_name__     = None
    __fields__          = None
    __keys__            = None
    __capabilities__    = None

    @staticmethod
    def from_announce(announce):
        obj = ManifoldObject

        table = announce.get_table()
        obj.__object_name__    = table.get_name()
        obj.__fields__         = table.get_fields()
        obj.__keys__           = table.get_keys()
        obj.__capabilities__   = table.get_capabilities()

        return obj

    def copy(self):
        return copy.deepcopy(self)

    @classmethod
    def get_object_name(cls):
        if cls.__doc__:
            announce = cls.get_announce()
            return announce.get_table().get_name()
        else:
            return cls.__object_name__ if cls.__object_name__ else cls.__name__

    @classmethod
    def get_fields(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_fields()
        else:
            return cls.__fields__

    @classmethod
    def get_keys(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_keys()
        else:
            return cls.__keys__

    def get_capabilities(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_capabilities()
        else:
            return cls.__capabilities__

    @classmethod
    def get_capabilities(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_capabilities()
        else:
            return cls.__capabilities__

    @classmethod
    def get_announce(cls):
        # The None value corresponds to platform_name. Should be deprecated # soon.
        if cls.__doc__:
            announce, = Announces.from_string(cls.__doc__, None)
        else:
            table = Table(None, cls.get_object_name(), cls.get_fields(), cls.get_keys())
            table.set_capabilities(cls.get_capabilities())
            #table.partitions.append()
            announce = Announce(table)
        return announce

# XXX It is still used ?
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
        Log.tmp("gateway object aliases = ",cls.aliases)
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

