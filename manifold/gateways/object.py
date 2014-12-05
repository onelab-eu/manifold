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
from manifold.core.object           import Object
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
from manifold.core.table            import Table
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.type             import accepts, returns 


class ManifoldCollection(set):

    def __init__(self, cls = None):
        if cls:
            self._cls = cls
        elif self.__doc__:
            announce, = Announces.from_string(self.__doc__, None)
            self._cls = Object.from_announce(announce)
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

    def create(self, *args, **kwargs):
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

    def create(self, obj):
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
