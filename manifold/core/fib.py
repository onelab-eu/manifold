import uuid

from manifold.core.announce         import Announce, Announces
from manifold.core.dbnorm           import Fd, Fds, Determinant, closure
from manifold.core.destination      import Destination
from manifold.core.field_names      import FieldNames
from manifold.core.filter           import Filter
from manifold.core.key              import Key
from manifold.core.keys             import Keys
from manifold.core.method           import Method
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.relation         import Relation
from manifold.core.table            import Table
from manifold.gateways              import LOCAL_NAMESPACE
from manifold.util.log              import Log
from manifold.util.predicate        import Predicate, eq

# This is somehow and object
class PlatformInfo(object):
    def __init__(self):
        self._object_name = None
        self._fields = dict()
        self._partitions = None
        self._capabilities = None

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

class Object(object):
    """
    A Manifold object.
    """
    def __init__(self, object_name):
        self._object_name = object_name
        self._fields = dict()
        self._keys = Keys()
        self._platform_info = dict()  # String -> PlatformInfo()
        self._relations = dict() # object_name -> relation

    # XXX Looks like an announce
    def __str__(self):
        object_name = self.get_object_name()
        relations = list()
        for o, r in self.get_relation_tuples():
            s = "REFERENCES %s AS %r" % (o, r)
            relations.append(s)
        relations_str = "\n\t".join(relations)
        fields = "\n\t".join([str(f) for f in self.get_fields()])
        keys = "\n\t".join([str(k) for k in self.get_keys()])
        platform_names = ", ".join(self.get_platform_names())
        CLASS_STR = "CLASS %(object_name)s (\n\t%(keys)s\n\t%(fields)s\n\t%(relations_str)s\n\t@ %(platform_names)s\n);\n\n"
        return CLASS_STR % locals()

    def get_object_name(self):
        return self._object_name

    def get_announce(self):
        fields = set(self.get_fields())
        t = Table(self.get_platform_names(), self.get_object_name(), self.get_fields(), self.get_keys())
        
        # XXX We hardcode table capabilities
        t.capabilities.retrieve   = True
        t.capabilities.join       = True
        t.capabilities.selection  = True
        t.capabilities.projection = True

        return Announce(t)
        

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

    def get_platform_names(self):
        return self._platform_info.keys()

    def get_name(self):
        return self._object_name

    def get_keys(self):
        return self._keys

    def get_fields(self):
        return self._fields.values()

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

class FIB(ChildSlotMixin):
    """
    Forwarding Information Base.
    Replaces DBNorm and DBGraph.
    """

    def __init__(self):
        ChildSlotMixin.__init__(self)

        # Tables indexed by name
        self._objects_by_namespace = dict()   # namespace -> (object_name -> object)

        # We store all accepted FDs
        self._fds = Fds()

        self._uuid = str(uuid.uuid4())

    # All receivers should have a UUID for destination
    def get_uuid(self):
        return self._uuid

    def get_address(self):
        return Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._uuid)))

    def get_namespaces(self):
        return self._objects_by_namespace.keys()

    def get_relation_tuples(self):
        ret = list()
        for src_object_name in self._objects_by_namespace[None].values():
            for dest_object_name, relation in src_object_name.get_relation_tuples():
                ret.append( (src_object_name, dest_object_name, relation) )
        return ret

    def get_objects(self, namespace = None):
        if not namespace in self._objects_by_namespace:
            return list()
        return self._objects_by_namespace[namespace].values()

    def get_object(self, object_name, namespace = None):
        return self._objects_by_namespace[namespace][object_name]

    def get_object_names(self, namespace = None):
        return self._objects_by_namespace[namespace].keys()

    def get_announces(self, namespace = None):
        # XXX All namespaces
        ret = list()
        #print "getting objects of namespace", namespace
        for obj in self.get_objects(namespace):
            ret.append(obj.get_announce())
        return ret

    def get_fds(self):
        return self._fds.copy()

    def add(self, platform_name, announces, namespace = None):
        """
        Adds a new announce to the FIB.
        """
        if not isinstance(announces, Announces):
            announces = Announces([announces])

        if namespace in self._objects_by_namespace:
            object_dict = self._objects_by_namespace[namespace]
        else:
            object_dict = dict()
            self._objects_by_namespace[namespace] = object_dict

        for announce in announces:

            table = announce.get_table() # XXX

            object_name     = table.get_name() # XXX

            print "FIB ADD", namespace, object_name

            keys            = table.get_keys()
            fields          = table.get_fields()
            capabilities    = table.get_capabilities()
            relations       = set()

            # Object
            if object_name in object_dict:
                obj = object_dict[object_name]
            else:
                obj = Object(object_name)
                object_dict[object_name] = obj

            # Keys
            # XXX subkey can be sufficient
            for key in table.get_keys():
                if key.is_empty():
                    key = Key(fields, local=key.is_local())
                    

                for field in fields:

                    fd = Fd(Determinant(key, object_name), {field: set([Method(platform_name, object_name)])})

                    #print "Considering FD", fd

                    # Let's challenge the Fd
                    # 1) indirect relationships: x -> a
                    x  = key.get_fields()
                    if field not in x:
                        # Key fields are kept inside the object
                        g = self.get_fds()
                        x_plus = closure(x, g)
                        if field in x_plus:
                            # XXX Don't keep the Fd
                            # XXX Some processing needed
                            #print "Don't keep the FD"
                            # XXX obj.set_platform_object_name(platform_name, object_name) # XXX
                            continue
                        
                    # We keep the FD

                    # Multivalued dependencies...
                    if key.is_composite():
                        for b in x:
                            # Alternative key without b
                            x_b = Key([xi for xi in key.get_fields() if xi != b]) # x_b = x - b
                            # g2 = The set of FDs in the FIB
                            fd2 = fd.copy()                            #   with fd' = [(x - b) -> a]
                            fd2.set_key(x_b)

                            g  = self.get_fds()

                            x_b_plus = closure(x_b.get_fields(), g)            # compute (x - b)+ with repect to g'

                            if field in x_b_plus:
                                # replace [x -> a] by [(x - b) -> a]
                                fd = fd2 # XXX ??? XXX
                                #print "Reduce the FD"

                    # The key is kept if it determines at least one field
                    # We can expect that no platform announces such superkeys...

                    obj.add_field(field)

                    obj.add_platform_field(platform_name, field)

                    # This should be done only if we have added a new field, so
                    # that's why it is not out of the loop
                    # XXX To confirm
                    obj.set_platform_capabilities(platform_name, capabilities)
                    obj.set_platform_object_name(platform_name, object_name)

                obj.add_key(key)

                # New relations ?
                # FD: x -> a
                # FD: (x, y) -> a
                #
                # We only look at relations between the current object and those
                # containing a (or typed after the type of a)
                #
                # Child objects too should be considered having the parents'
                # fields
                #print "INFERRING RELATIONS FROM ", obj.get_object_name(), "TOWARDS others"
                # !! We cannot infer relations before we insert the key
                for other in self.get_objects():
                    relations = obj.infer_relations(other)
                    for relation in relations:
                        if not relation in obj.get_relations():
                            obj.add_relation(other.get_object_name(), relation)
                            #other.add_relation(obj.get_object_name(), relation.get_reverse())

                    relations = other.infer_relations(obj)
                    for relation in relations:
                        if not relation in other.get_relations():
                            other.add_relation(obj.get_object_name(), relation)
                            #obj.add_relation(other.get_object_name(), relation.get_reverse())
#DEPRECATED|
#DEPRECATED|                            print "BILAN DES RELATIONS BY object_name for namespace", namespace
#DEPRECATED|                            print "OBJ", obj.get_object_name()
#DEPRECATED|                            print self.get_object(obj.get_object_name(), namespace)
#DEPRECATED|                            print "OTHER", other.get_object_name()
#DEPRECATED|                            print self.get_object(other.get_object_name(), namespace)


            # Fields = they define a set of FD
            # We have new Fd that can impact other existing objects

            # A new FD:
            # 1) is it redundant ?
            # 2) does it make any relation redundant ?

    def receive(self, packet):
        platform_name = packet._ingress.get_filter().get_eq('uuid')
        namespace = 'local' if platform_name == 'local' else None

        announce = Announce(Table.from_dict(packet.to_dict(), platform_name))
        #print "ON_RECEIVE => FIB:add(%(platform_name)s, %(announce)s, %(namespace)s)" % locals()
        self.add(platform_name, announce, namespace)

    def dump(self):
        print "#" * 80
        print "FIB DUMP"
        print "#" * 80
        for namespace in self.get_namespaces():
            print ""
            print "NAMESPACE", namespace
            print ""
            print "=" * 80
            for obj in self.get_objects():
                print obj
            print "=" * 80
