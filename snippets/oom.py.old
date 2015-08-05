#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.filter import Filter
# fields are a set of string for the time being

#DEPRECATED|get_class = lambda x: globals()[x]

class BaseClass(object):
    """
    Base class for declaring Manifold objects (in gateways)
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, classtype):
        self._type = classtype

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_object(self):
        # XXX What we really need : CamelCase to camel_case
        return self.__class__.__name__.lower()

    #---------------------------------------------------------------------------
    # Static methods
    #---------------------------------------------------------------------------

    @classmethod
    def collection(cls, filter, fields):
        return cls.__collection__(cls.__object__, filter, fields)

def ClassFactory(name, argnames=None, BaseClass=BaseClass):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            # here, the argnames variable is the one passed to the
            # ClassFactory call
            if key not in argnames:
                raise TypeError("Argument %s not valid for %s" 
                    % (key, self.__class__.__name__))
            setattr(self, key, value)
        BaseClass.__init__(self, name[:-len("Class")])
    newclass = type(name, (BaseClass,),{"__init__": __init__})
    setattr(newclass, '__object__', name)
    return newclass

class Collection(object):
    """
    A collection is a lazy list of objects

    Consider:
    - Pointer to a common cache
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------
    
    def __init__(self, object, filter, fields):
        self._object = object
        self._filter = filter
        self._fields = fields
        
        self._cache  = None
        self._dirty  = True # We need to make a request to get content

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_filter(self):
        return self._filter

    def get_fields(self):
        return self._fields

    def get_cache(self):
        return self._cache

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    def __repr__(self):
        return "<Slice ...>"

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def update_cache(self):
        pass

    def __getitem__(self, key):
        if isinstance(key, int):
            #if self._dirty:
            #    self.update_cache()
            #return self.get_cache()[key]
            return self.get(self.get_filter(), self.get_fields())

        elif isinstance(key, StringTypes):
            raise Exception, "Not implemented"

    @classmethod
    def get_object_class(cls):
        object_cls = ClassFactory(cls.__object__)
        setattr(object_cls, '__collection__', cls)
        return object_cls


################################################################################
# What should be defined in a gateway
################################################################################

class SliceCollection(Collection):

    __object__ = 'slice'

    def get(self, filter, fields):
        print "Here we issue the request"
        class Slice(object): pass
        o = Slice()
        setattr(o, 'slice_hrn', 'this is a test')
        return o


Slice = SliceCollection.get_object_class()

################################################################################
# Sample script
################################################################################

print "Collection associated to Slice=", Slice.__collection__

sc = Slice.collection({'slice_hrn', '==', 'ple.upmc.myslicedemo'}, ['slice_hrn'])
print "object declared - sc=", sc

s = sc[0]

print "s=", s
print "s['slice_hrn']=", s.slice_hrn # ['slice_hrn']
