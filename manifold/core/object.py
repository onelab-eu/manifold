# -*- coding: utf-8 -*-

import copy

from manifold.core.announce         import Announce, Announces
from manifold.core.capabilities     import Capabilities
from manifold.core.field_names      import FieldNames
from manifold.core.keys             import Keys
from manifold.core.partition        import Partitions
from manifold.core.record           import Record
from manifold.core.relation         import Relation
from manifold.core.table            import Table
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.predicate        import Predicate, eq

# This is somehow a Object
# All this should replace Table sooner or later
class PlatformInfo(object):
    def __init__(self):
        self._object_name = None
        self._fields = dict()
        self._capabilities = None
        self._partitions = Partitions()

    def get_object_name(self):
        return self._object_name

    def set_object_name(self, object_name):
        self._object_name = object_name

    def add_field(self, field):
        if field.get_name() in self._fields:
            Log.debug("duplicate field")
            return
        self._fields[field.get_name()] = field

    def get_fields(self):
        return self._fields.values()

    def get_field_names(self):
        return FieldNames(self._fields.keys())

    def set_capabilities(self, capabilities):
        self._capabilities = capabilities

    def get_capabilities(self):
        return self._capabilities

    def add_partition(self, partition):
        self._partitions.add(partition)

    def add_partitions(self, partitions):
        self._partitions |= partitions

    def get_partitions(self):
        return self._partitions

class Object(Record):
    """
    A Manifold object.
    """
    __metaclass__   = PluginFactory
    __plugin__name__attribute__ = '__object_name__'

    __object_name__     = None
    __fields__          = None
    __keys__            = None
    __capabilities__    = None
    __partitions__      = None
    __namespace__       = None


    # META 
    @classmethod
    def collection(cls, *args, **kwargs):
        o1 = cls()
        o2 = cls()
        o1.log = "O1 LOG"
        o2.log = "O2 log"
        return [o1, o2]
    

    @staticmethod
    def from_announce(announce):
        table = announce.get_table()

        obj = ObjectFactory(table.get_name())
        obj.set_fields(table.get_fields())
        obj.set_keys(table.get_keys())
        obj.set_capabilities(table.get_capabilities())
        obj.set_partitions(table.get_partitions())
        
        return obj

    def copy(self):
        return copy.deepcopy(self)

    @classmethod
    def get_object_name(cls):
        return cls.__object_name__ if cls.__object_name__ else cls.__name__

    @classmethod
    def set_namespace(cls, namespace):
        cls.__namespace__ = namespace

    @classmethod
    def get_namespace(cls):
        return cls.__namespace__

    @classmethod
    def get_fields(cls):
        return cls.__fields__.values()

    @classmethod
    def get_field_names(cls):
        return FieldNames(cls.__fields__.keys())

    @classmethod
    def get_keys(cls):
        return cls.__keys__

    @classmethod
    def get_capabilities(cls):
        return cls.__capabilities__

    @classmethod
    def get_partitions(cls):
        return cls.__partitions__

    @classmethod
    def get_announce(cls):
        # The None value corresponds to platform_name. Should be deprecated # soon.
#        if cls.__doc__:
#            announce, = Announces.from_string(cls.__doc__, None)
#        else:
        table = Table(None, cls.get_object_name(), cls.get_fields(), cls.get_keys())
        table.set_capabilities(cls.get_capabilities())
        table.add_partitions(cls.get_partitions())
        table.set_namespace(cls.get_namespace())
        #table.partitions.append()
        announce = Announce(table)
        return announce

    # XXX Looks like an announce
    def __str__(self):
        object_name = self.get_object_name()
        relations = list()
        for o, r in self.get_relation_tuples():
            s = "REFERENCES %s AS %r" % (o, r)
            relations.append(s)
        relations_str = "\n\t".join(relations)
        partitions_str = "PARTITION BY (%s)" % ("; ".join(self.get_partitions()),)
        fields = "\n\t".join([str(f) for f in self.get_fields()])
        keys = "\n\t".join([str(k) for k in self.get_keys()])
        platform_names = ", ".join(self.get_platform_names())
        CLASS_STR = "CLASS %(object_name)s (\n\t%(keys)s\n\t%(fields)s\n\t%(relations_str)s\n\t%(partitions_str)s\n\t@ %(platform_names)s\n);\n\n"
        return CLASS_STR % locals()

#    def get_object_name(self):
#        return cls.__object_name__
#
#    def get_announce(self):
#        fields = set(self.get_fields())
#        t = Table(self.get_partitions(), self.get_object_name(), self.get_fields(), self.get_keys())
#        
#        # XXX We hardcode table capabilities
#        t.capabilities.retrieve   = True
#        t.capabilities.join       = True
#        t.capabilities.selection  = True
#        t.capabilities.projection = True
#
#        return Announce(t)

    @classmethod
    def remove_platform(cls, platform_name):
        if platform_name in cls.__platform_info__:
            del cls.__platform_info__[platform_name]

    @classmethod
    def get_platform_object_name(cls, platform_name):
        return cls.__platform_info__[platform_name].get_object_name()

    @classmethod
    def set_platform_object_name(cls, platform_name, object_name):
        if not platform_name in cls.__platform_info__:
            cls.__platform_info__[platform_name] = PlatformInfo()
        cls.__platform_info__[platform_name].set_object_name(object_name)

    @classmethod
    def add_field(cls, field):
        if field.get_name() in cls.__fields__:
            Log.debug("duplicate field")
            return
        cls.__fields__[field.get_name()] = field

    @classmethod
    def add_fields(cls, fields):
        for field in fields:
            cls.add_field(field)

    @classmethod
    def set_fields(cls, fields):
        cls.__fields__ = dict()
        cls.add_fields(fields)

    @classmethod
    def add_key(cls, key):
        cls.__keys__.add(key)

    @classmethod
    def add_keys(cls, keys):
        for key in keys:
            cls.add_key(key)

    @classmethod
    def set_keys(cls, keys):
        cls.__keys__ = Keys()
        cls.add_keys(keys)

    @classmethod
    def add_platform_field(cls, platform_name, field):
        if not platform_name in cls.__platform_info__:
            cls.__platform_info__[platform_name] = PlatformInfo()
        cls.__platform_info__[platform_name].add_field(field)

    @classmethod
    def get_platform_fields(cls, platform_name):
        return cls.__platform_info__[platform_name].get_fields()

    @classmethod
    def get_platform_object_name(cls, platform_name):
        return cls.__platform_info__[platform_name].get_object_name()

    @classmethod
    def get_platform_field_names(cls, platform_name):
        return FieldNames(cls.__platform_info__[platform_name].get_field_names())

    @classmethod
    def set_platform_capabilities(cls, platform_name, capabilities):
        if not platform_name in cls.__platform_info__:
            cls.__platform_info__[platform_name] = PlatformInfo()
        cls.__platform_info__[platform_name].set_capabilities(capabilities)

    @classmethod
    def get_platform_capabilities(cls, platform_name):
        return cls.__platform_info__[platform_name].get_capabilities()

    @classmethod
    def add_partition(cls, partition):
        cls.__partitions__.add(partition)

    @classmethod
    def add_partitions(cls, partitions):
        cls.__partitions__ |= partitions

    @classmethod
    def set_partitions(cls, partitions):
        cls.__partitions__ = Partitions()
        cls.add_partitions(partitions)

#    def get_partitions(cls):
#        return cls.__partitions__

    @classmethod
    def add_platform_partition(cls, platform_name, partition):
        if not platform_name in cls.__platform_info__:
            cls.__platform_info__[platform_name] = PlatformInfo()
        cls.__platform_info__[platform_name].add_partition(partition)

    @classmethod
    def add_platform_partitions(cls, platform_name, partitions):
        if not platform_name in cls.__platform_info__:
            cls.__platform_info__[platform_name] = PlatformInfo()
        cls.__platform_info__[platform_name].add_partitions(partitions)

    @classmethod
    def get_platform_partitions(cls, platform_name):
        return cls.__platform_info__[platform_name].get_partitions()

    @classmethod
    def get_platform_names(cls):
        return cls.__platform_info__.keys()

    @classmethod
    def set_capabilities(cls, capabilities):
        cls.__capabilities__ = capabilities

    @classmethod
    def get_name(cls):
        return cls.__object_name__
    # Inference helpers

    @classmethod
    def get_common_keys_with(cls, other_object):
        return cls.get_keys().intersection(other_object.get_keys())

    @classmethod
    def infer_relations(cls, other_object):
        u = cls
        v = other_object
        relations = set()

        if u.get_name() == v.get_name():
            # Relation to myself ?
            return relations

        #        LINK_NN
        #      /        \
        # LINK_1N     LINK_N1
        #       \       /  \
        #        LINK_11 ___\ 
        #           |        \
        #        SIBLINGS    SHORTCUT?
        #        /      \
        #     PARENT   CHILD
        # First trying to see whether we have Parent/Child/Siblings

        # (a) The key of one object refers to the type of another object
        for key in u.get_keys():
            if key.is_composite():
                continue
            key_field_type = key.get_field_type()
            if key_field_type == v.get_name():
                for v_key in v.get_keys():
                    predicate = Predicate(key.get_field_name(), eq, v_key.get_field_name())
                    relations.add(Relation(Relation.types.CHILD, predicate))
                    # We don't name the relation since it could not be unique,
                    # and we could have two relations with the same name between
                    # 2 objects... (and ? what is the problem ?)
                    # , name=v.get_name()))

        # (b) The two objects have the same key
        common_keys = u.get_common_keys_with(v)
        if common_keys:
            # Three possibilities:
            #     SIBLINGS
            #     /      \
            #  PARENT   CHILD
            for common_key in common_keys:
                key_field_names = common_key.get_field_names()
                predicate = Predicate(key_field_names, eq, key_field_names)
                # We need to be sure we can use the predicate (we have the
                # necessary fields) (even indirectly ?)
                relations.add(Relation(Relation.types.SIBLING, predicate))

        # (c) A field of u points to v
        # Detect explicit Relation from u to v
        for field in u.get_fields():
            if field.get_type() == v.get_name():
                for v_key in v.get_keys():
                    if v_key.is_composite():
                        # We assume that u (for ex: traceroute) provides in the current field (ex: hops)
                        # a record containing at least the v's key (for ex: (agent, destination, first, ttl))
                        intersecting_fields = tuple(u.get_field_names() & v_key.get_field_names())
                        predicate = Predicate(intersecting_fields, eq, intersecting_fields)
                    else:
                        predicate = Predicate(field.get_name(), eq, v_key.get_field_name())

                    if field.is_array():
                        relations.add(Relation(Relation.types.LINK_NN, predicate, name=field.get_name(), local = v_key.is_local())) # LINK_1N_FORWARD
                    else:
                        #if field.is_local():
                        #    relations.add(Relation(Relation.types.LINK_11, predicate, name=field.get_name()))
                        #else:
                        relations.add(Relation(Relation.types.LINK_11, predicate, name=field.get_name()))

        # NOTE: some relations could be included in others if they refine them
        # we should only keep the finest relations for each pair of tables

        return relations

    @classmethod
    def get_relations(cls):
        ret = list()
        for _, relation_list in cls.__relations__.items():
            ret.extend(relation_list)
        return ret

    @classmethod
    def get_relation_tuples(cls):
        ret = list()
        for object_name, relation_list in cls.__relations__.items():
            for relation in relation_list:
                ret.append( (object_name, relation) )
        return ret

    @classmethod
    def add_relation(cls, other_object_name, relation):
        Log.debug("DUPLICATE RELATIONS!")
        if not other_object_name in cls.__relations__:
            cls.__relations__[other_object_name] = set()
        relation.set_uuid()
        cls.__relations__[other_object_name].add(relation)

    def __eq__(self, other):
        Log.critical("Object equality not implemented yet.")
        # XXX rely on equality of keys
        return object.__eq__(self, other)

    def __hash__(self):
        Log.critical("Object hash not implemented yet.")
        # XXX hash on keys needed without order, see Key class
        return object.__hash__(self)

def ObjectFactory(name): 
    def __init__(self, **kwargs):
        Object.__init__(self, **kwargs)
        for key, value in kwargs.items():
            # here, the argnames variable is the one passed to the
            # ClassFactory call
            
            if not self.__fields__ or key not in self.__fields__.keys():
                raise TypeError("Argument %s not valid for %s" 
                    % (key, self.__class__.__name__))
            self[key] = value
            #setattr(cls, key, value)
        #Object.__init__(cls) # , name)
    newclass = type(str(name), (Object,),{"__init__": __init__})

    newclass.__object_name__     = name
    newclass.__fields__          = dict()
    newclass.__keys__            = Keys()
    newclass.__capabilities__    = Capabilities()
    newclass.__partitions__      = Partitions()
    newclass.__namespace__       = None

    # That is only for FIB

    newclass.__relations__       = dict() # object_name -> relation 
    newclass.__platform_info__   = dict() # String -> PlatformInfo()    

    return newclass
