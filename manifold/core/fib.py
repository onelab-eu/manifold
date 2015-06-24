import uuid

from manifold.core.announce         import Announce, Announces
from manifold.core.dbnorm           import Fd, Fds, Determinant, closure
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.key              import Key
from manifold.core.keys             import Keys
from manifold.core.method           import Method
from manifold.core.object           import Object, ObjectFactory
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.table            import Table
from manifold.util.log              import Log
from manifold.util.predicate        import Predicate

class FIB(ChildSlotMixin):
    """
    Forwarding Information Base.
    Replaces DBNorm and DBGraph.
    """

    def __init__(self, router):
        ChildSlotMixin.__init__(self)

        # Tables indexed by name
        self._objects_by_namespace = dict()   # namespace -> (object_name -> object)

        # We store all accepted FDs
        self._fds = Fds()

        self._uuid = str(uuid.uuid4())

        self._router = router

    def is_up(self, interface_name):
        return self._router.is_interface_up(interface_name)

    # All receivers should have a UUID for destination
    def get_uuid(self):
        return self._uuid

    def get_address(self):
        return Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._uuid)))

    def get_namespaces(self):
        namespaces = self._objects_by_namespace.keys()
        return [n for n in namespaces if n is not None]

#DEPRECATED|    def get_object_relation_tuples(self, obj):
#DEPRECATED|        ret = list()
#DEPRECATED|        for dest_object_name, relation in obj.get_relation_tuples():
#DEPRECATED|            ret.append( (src_object_name, dest_object_name, relation) )
#DEPRECATED|        return ret

    def get_relation_tuples(self, object_name = None, namespace = None):
        ret = list()
        for obj in self.get_objects():
            ret.extend(obj.get_relation_tuples())
        return ret


    def get_objects_from_namespace(self, namespace = None):
        if not namespace in self._objects_by_namespace:
            return list()
        return self._objects_by_namespace[namespace].values()

    def get_objects(self, namespace = None):
        if namespace == '*':
            ret = list()
            for namespace in self.get_namespaces():
                ret.extend(self.get_objects_from_namespace(namespace))
            return ret
        else:
            return self.get_objects_from_namespace(namespace)

    def get_object(self, object_name, namespace = None):
        return self._objects_by_namespace[namespace][object_name]

    def get_object_names(self, namespace = None):
        return self._objects_by_namespace[namespace].keys()

    def get_announces_from_namespace(self, namespace):
        ret = list()
        for obj in self.get_objects(namespace):
            ret.append(obj.get_announce())
        return ret

    def get_announces(self, namespace = None):
        if namespace == '*':
            ret = list()
            for namespace in self.get_namespaces():
                ret.extend(self.get_announces_from_namespace(namespace))
            return ret
        else:
            return self.get_announces_from_namespace(namespace)

    def get_fds(self):
        return self._fds.copy()

    def add(self, platform_name, announces, namespace = None): # XXX namespace should be in announced object 
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
            #print "FIB RECEIVED OBJECT: %s:%s" % (object_name, namespace)

            keys            = table.get_keys()
            fields          = table.get_fields()
            capabilities    = table.get_capabilities()

            #obj.add_capabilities(partitions)
            #obj.add_platform_capabilities(platform_name, partitions)

            partitions      = table.get_partitions()

            relations       = set()

            # Object
            if object_name in object_dict:
                obj = object_dict[object_name]
            else:
                obj = ObjectFactory(object_name)
                obj.set_namespace(namespace)
                object_dict[object_name] = obj

            obj.add_partitions(partitions)
            obj.add_platform_partitions(platform_name, partitions)

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
                # XXX what the capability for the joined object ?
                obj.set_capabilities(capabilities)

                # New relations ?
                # FD: x -> a
                # FD: (x, y) -> a
                #
                # We only look at relations between the current object and those
                # containing a (or typed after the type of a)
                #
                # Child objects too should be considered having the parents'
                # fields
                # !! We cannot infer relations before we insert the key
                for other in self.get_objects(namespace):
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

        if packet.is_empty():
            return

        data = packet.get_data()

        # IDEAL, see manifold.core.local before
        # announce = Announce.from_dict(packet.to_dict(), platform_name)

        announce = Announce(Table.from_dict(data, platform_name))
        obj = announce.get_object()
        namespace = obj.get_namespace()
        
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
            for obj in self.get_objects(namespace):
                print obj
            print "=" * 80

    def remove_platform(self, platform_name):
        for namespace in self.get_namespaces():
            for obj in self.get_objects(namespace):
                obj.remove_platform(platform_name)
