# -*- coding: utf-8 -*-

from manifold.core.announce         import Announce, Announces
from manifold.core.field_names      import FieldNames
from manifold.core.keys             import Keys
from manifold.core.partition        import Partitions
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
            Log.warning("duplicate field")
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

class Object(object):
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

    @staticmethod
    def from_announce(announce):
        class obj(Object):
            pass

        table = announce.get_table()
        obj.__object_name__    = table.get_name()
        obj.__fields__         = table.get_fields()
        obj.__keys__           = table.get_keys()
        obj.__capabilities__   = table.get_capabilities()
        obj.__partitions__     = table.get_partitions()
        
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
            announce = cls.get_announce()
            return announce.get_table().get_fields()
        else:
            return cls.__fields__

    @classmethod
    def get_keys(cls):
        if cls.__doc__:
            announce = cls.get_announce()
            return announce.get_table().get_keys()
        else:
            return cls.__keys__

    @classmethod
    def get_capabilities(cls):
        if cls.__doc__:
            announce = cls.get_announce()
            return announce.get_table().get_capabilities()
        else:
            return cls.__capabilities__

    @classmethod
    def get_partitions(cls):
        if cls.__doc__:
            announce = cls.get_announce()
            return announce.get_table().get_partitions()
        else:
            return cls.__partitions__

    @classmethod
    def get_announce(cls):
        import pdb; pdb.set_trace()
        # The None value corresponds to platform_name. Should be deprecated # soon.
        if cls.__doc__:
            announce, = Announces.from_string(cls.__doc__, None)
        else:
            table = Table(None, cls.get_object_name(), cls.get_fields(), cls.get_keys())
            table.set_capabilities(cls.get_capabilities())
            table.add_partitions(cls.get_partitions())
            table.set_namespace(cls.get_namespace())
            #table.partitions.append()
            announce = Announce(table)
        return announce

    @classmethod
    def get_namespace(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_namespace()
        else:
            return cls.__namespace__

    def __init__(self, object_name, namespace = None):
        self._object_name = object_name
        self._fields = dict()
        self._keys = Keys()
        self._platform_info = dict()  # String -> PlatformInfo()
        self._relations = dict() # object_name -> relation
        self._partitions = Partitions()
        self._namespace = namespace

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
#        return self._object_name
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

    def remove_platform(self, platform_name):
        if platform_name in self._platform_info:
            del self._platform_info[platform_name]

    def get_platform_object_name(self, platform_name):
        return self._platform_info[platform_name].get_object_name()

    def set_platform_object_name(self, platform_name, object_name):
        if not platform_name in self._platform_info:
            self._platform_info[platform_name] = PlatformInfo()
        self._platform_info[platform_name].set_object_name(object_name)

    def add_field(self, field):
        if field.get_name() in self._fields:
            Log.warning("duplicate field")
            return
        self._fields[field.get_name()] = field

    def add_key(self, key):
        self._keys.add(key)

    def add_platform_field(self, platform_name, field):
        if not platform_name in self._platform_info:
            self._platform_info[platform_name] = PlatformInfo()
        self._platform_info[platform_name].add_field(field)

    def get_platform_fields(self, platform_name):
        return self._platform_info[platform_name].get_fields()

    def get_platform_object_name(self, platform_name):
        return self._platform_info[platform_name].get_object_name()

    def get_platform_field_names(self, platform_name):
        return FieldNames(self._platform_info[platform_name].get_field_names())

    def set_platform_capabilities(self, platform_name, capabilities):
        if not platform_name in self._platform_info:
            self._platform_info[platform_name] = PlatformInfo()
        self._platform_info[platform_name].set_capabilities(capabilities)

    def get_platform_capabilities(self, platform_name):
        return self._platform_info[platform_name].get_capabilities()

    def add_partition(self, partition):
        self._partitions.add(partition)

    def add_partitions(self, partitions):
        self._partitions |= partitions

#    def get_partitions(self):
#        return self._partitions

    def add_platform_partition(self, platform_name, partition):
        if not platform_name in self._platform_info:
            self._platform_info[platform_name] = PlatformInfo()
        self._platform_info[platform_name].add_partition(partition)

    def add_platform_partitions(self, platform_name, partitions):
        if not platform_name in self._platform_info:
            self._platform_info[platform_name] = PlatformInfo()
        self._platform_info[platform_name].add_partitions(partitions)

    def get_platform_partitions(self, platform_name):
        return self._platform_info[platform_name].get_partitions()

    def get_platform_names(self):
        return self._platform_info.keys()

    def get_name(self):
        return self._object_name

#    def get_keys(self):
#        return self._keys
#
#    def get_fields(self):
#        return self._fields.values()

    def get_field_names(self):
        return FieldNames(self._fields.keys())

    # Inference helpers

    def get_common_keys_with(self, other_object):
        return self.get_keys().intersection(other_object.get_keys())

    def infer_relations(self, other_object):
        u = self
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

    def get_relations(self):
        ret = list()
        for _, relation_list in self._relations.items():
            ret.extend(relation_list)
        return ret

    def get_relation_tuples(self):
        ret = list()
        for object_name, relation_list in self._relations.items():
            for relation in relation_list:
                ret.append( (object_name, relation) )
        return ret

    def add_relation(self, other_object_name, relation):
        Log.warning("DUPLICATE RELATIONS!")
        if not other_object_name in self._relations:
            self._relations[other_object_name] = set()
        relation.set_uuid()
        self._relations[other_object_name].add(relation)

