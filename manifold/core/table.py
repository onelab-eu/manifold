#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Table:
# Stores the representation of a table in the 3-nf graph
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr>

from copy                       import deepcopy
from types                      import StringTypes
import uuid

from manifold.core.field        import Field
from manifold.core.fields       import Fields
from manifold.types             import BASE_TYPES
from manifold.core.filter       import Filter
from manifold.core.key          import Key, Keys
from manifold.core.method       import Method
from manifold.core.capabilities import Capabilities
from manifold.core.relation     import Relation
from manifold.util.type         import returns, accepts
from manifold.util.log          import Log
from manifold.util.predicate    import Predicate, eq

class Table(object):
    """
    Implements a database table schema.
    """

    #-----------------------------------------------------------------------
    # Internal usage
    #-----------------------------------------------------------------------

    @staticmethod
    def check_fields(fields):
        """
        Check whether fields parameter is well-formed in __init__
        """
        if fields == None:
            return False
        elif isinstance(fields, (set, frozenset, list, tuple)):
            for field in fields:
                if not isinstance(field, Field):
                    raise TypeError("In fields = %r: %r is not of type Field" % (fields, field))
        elif isinstance(fields, dict):
            for field_name, metafield in fields.items():
                if not isinstance(field_name, StringTypes):
                    raise TypeError("In fields = %r: %r is not of type StringTypes" % (fields, field_name))
                elif not isinstance(metafield, Field):
                    raise TypeError("In fields = %r: %r is not of type Field" % (fields, metafield))

    @staticmethod
    def check_keys(keys):
        """
        Check whether keys parameter is well-formed in __init__
        """
        if not keys: return
        if not isinstance(keys, (tuple, list, set, frozenset)):
            raise TypeError("keys = %r is not of type tuple, list, frozenset" % keys)
        for key in keys:
            if not isinstance(key, (tuple, list, set, frozenset)):
                raise TypeError("In keys = %r: %r is not of type Key (type %r)" % (keys, key, type(key)))
            for key_elt in key:
                if not isinstance(key_elt, (StringTypes,Field)):
                    raise TypeError("In key %r: %r is not of type StringTypes" % (key, key_elt))

    @staticmethod
    def check_partitions(partitions):
        """
        Check whether keys parameter is well-formed in __init__
        """
        if isinstance(partitions, StringTypes):
            return
        elif isinstance(partitions, (list, set, frozenset)):
            # partitions carries a set of platforms
            for platform in partitions:
                if not isinstance(platform, StringTypes):
                   raise TypeError("In partitons = %r: platform = %r is not of type StringTypes" % (partitions, platform))
        elif isinstance(partitions, dict):
            for platforms, clause in partitions.items():
                if platforms and not isinstance(platforms, frozenset):
                    return TypeError("platforms = %r is not of type frozenset" % platforms)
                if clause and not isinstance(clause, Predicate):
                    return TypeError("clause = %r is not of type Predicate" % clause)

    @staticmethod
    def check_init(partitions, name, fields, keys):
        """
        Check whether parameters passed to __init__ are well formed
        """
        Table.check_fields(fields)
        Table.check_keys(keys)
        Table.check_partitions(partitions)

    #-----------------------------------------------------------------------
    # Constructor
    #-----------------------------------------------------------------------

    def __init__(self, partitions, table_name, fields = None, keys = None):
        """
        Table constructor
        Args:
            partitions: It can be either:
                - a String (the name of the platform)
                - a dictionary {String => Predicate} where each key is the name
                of a platform and each data is a Predicate or "None". "None" means
                that the condition is always True.
                - a set/list of platform names
            table_name: The name of the table
            fields: A set/list of Fields involved in the table or None
            keys: A set of Key instances or None
        """
        # Check parameters
        Table.check_init(partitions, table_name, fields, keys)
        self.set_partitions(partitions)

        # Init self.fields
        self.fields = dict()
        if isinstance(fields, (list, set, frozenset)):
            assert len(fields) > 0
            for field in fields:
                self.insert_field(field)
        elif isinstance(fields, dict):
            self.fields = fields

        # Init self.keys
        self.keys = Keys()
        if keys:
            for key in keys:
                self.insert_key(key)

        # Init self.platforms
        self.init_platforms()

        # Init default capabilities (none)
        self.capabilities = Capabilities()

        # Init self.name.
        # Enforce unicode encoding to guarantee format consistency among all Table instances.
        self.name = unicode(table_name)

    #-----------------------------------------------------------------------
    # Outputs
    #-----------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Table.
        """
        return "{%s}::%s {\n\t%s;\n\n\t%s;\n\t%s\n};" % (
            ', '.join([p          for p in sorted(self.get_platforms())]),
            self.get_name(),
            ';\r\n    '.join(["%s%s" % (f, "[]" if f.is_array() else "") for f in sorted(self.get_fields())]),
#            '\n\t'.join(["%s;\t// via %r" % (field, methods) for field, methods in self.map_field_methods.items()]),
            '\r\n    ;'.join(["%s" % k for k in self.get_keys()]),
            self.get_capabilities()
        )

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Table.
        """
        platforms = self.get_platforms()
        if platforms:
            return "<{%s}::%s>" % (', '.join([p for p in sorted(platforms)]), self.get_name())
        else:
            return self.get_name()

    #-----------------------------------------------------------------------
    # Methods
    #-----------------------------------------------------------------------

    @returns(StringTypes)
    def get_name(self):
        """
        Returns:
            A String storing the table name of this Table.
        """
        return self.name

    @returns(bool)
    def has_key(self, key):
        """
        Test whether a field is a key of this Table.
        Args:
            key: A Key instance.
        Returns:
            True iif only this is a key.
        """
        if not isinstance(key, Key):
            raise TypeError("Invalid key = %r (%r)", (key, type(key)))
        return key in self.keys

    def insert_field(self, field):
        """
        Add a field in self.
        Args:
            field: A Field instance
        """
        self.fields[field.get_name()] = field

    @returns(set)
    def get_fields(self):
        """
        Returns:
            The set of Field instances related to this Table.
        """
        return set(self.fields.values())

    @returns(Field)
    def get_field(self, field_name):
        """
        Retrieve a Field according to its name.
        Args:
            field_name: A String instance storing a field name of this Table.
        Raises:
            KeyError: if the field_name does not refer to a Field of this Table.
        Returns:
            The Field instance corresponding to this field name.
        """
        return self.fields[field_name]

    @returns(StringTypes)
    def get_field_type(self, field_name):
        """
        Retrieve the type of a Field of this Table.
        Args:
            field_name: A String instance containing the field name of a
                Field contained in this Table.
        Raises:
            KeyError: if the field_name does not refer to a Field of this Table.
        Returns:
            A String containing the type of this Field.
        """
        return self.get_field(field_name).get_type()

    @returns(bool)
    def erase_field(self, field_name):
        """
        Remove a Field from the table
        Args:
            field_name: A String containing the name of the
                field we want to remove.
        Raises:
            KeyError: if the field_name does not refer to a Field of this Table.
        Returns:
            True iif the field has been successfully removed
        """
        ret = field_name in self.fields
        del(self.fields[field_name])
        return ret

    def insert_key(self, key, local = None):
        """
        Add a Key in this Table.
        Args:
            key: The new key. Supported types are
                Key
                Field      (a Field instance belonging to this Table)
                StringType (a field name, related to a field of this Table)
                container  (list, set, frozenset, tuple) made of StringType (field names)
        Raises:
            TypeError: if the key argument is not valid.
        """
        if local:
            self.keys.set_local()

        if key is None:
            return

        if isinstance(key, Key):
            if local:
                key.set_local()
            self.keys.add(key)
        else:
            if isinstance(key, Field):
                fields = frozenset([key])
            elif isinstance(key, StringTypes):
                fields = frozenset([self.get_field(key)])
            elif isinstance(key, (list, set, frozenset, tuple)):
                fields = frozenset([self.get_field(key_elt) for key_elt in key])
            else:
                raise TypeError("key = %r is not of type Key nor Field nor StringTypes")
            self.keys.add(Key(fields, local = local))

    def set_capability(self, capability):
        if isinstance(capability, Capabilities):
            self.capabilities = capability
            return
        if isinstance(capability, StringTypes):
            capability = [capability]
        elif isinstance(capability, (list, tuple, set, frozenset)):
            capability = list(capability)
        else:
            raise TypeError("capability = %r is not of type String or iterable")
        for c in capability:
            setattr(self.capabilities, c, True)

    @returns(bool)
    def erase_key(self, key):
        """
        Remove a Key for this Table.
        Args:
            key: A Key instance.
                This Key must refer Field instance(s) of this Table.
        Returns:
            True iif the Key has been found and successfully removed.
        """
        l = len(list(self.keys))
        keys = Keys()
        for k in self.get_keys():
            if k != key:
                keys.add(k)
        self.keys = keys
        return l != len(list(self.keys))

    def erase_keys(self):
        """
        Remove every Key for this table
        """
        self.keys = Keys()

#DEPRECATED|    def insert_methods(self, methods):
#DEPRECATED|        """
#DEPRECATED|        Add a pseudo method for every field of the table
#DEPRECATED|        Args:
#DEPRECATED|            method A Method instance
#DEPRECATED|        """
#DEPRECATED|        # update self.map_field_methods
#DEPRECATED|        for field in self.map_field_methods.keys():
#DEPRECATED|            key = Key([field])
#DEPRECATED|            if not self.has_key(key):
#DEPRECATED|                self.map_field_methods[field] |= methods
#DEPRECATED|
#DEPRECATED|        # update self.platforms
#DEPRECATED|        for method in methods:
#DEPRECATED|            self.platforms.add(method.get_platform())

    def get_field(self, field_name):
        """
        Retrieve a Field instance stored in self according to
            its field name.
        Args:
            field_name: The requested field (String or Field instance).
        Returns:
            The Field instance identified by 'field_name'
        """
        # Retrieve the field name
        if isinstance(field_name, Field):
            field_name = field_name.get_name()
        elif not isinstance(field_name, StringTypes):
            raise TypeError("get_field: '%s' has an invalid type (%s): supported types are StringTypes and Field" % (field_name, type(field_name)))

        # Search the corresponding field
        try:
            return self.fields[field_name]
        except KeyError:
            raise ValueError("get_field: field '%s' not found in '%r'. Available fields: %s" % (field_name, self, self.get_fields()))

    @returns(set)
    def get_field_names(self):
        """
        Retrieve the field names of the fields stored in self.
        Returns:
            A set of Strings (one String per field name).
        """
        return Fields(self.fields.keys())

    @returns(Keys)
    def get_keys(self):
        """
        Returns:
            The Keys instance related to this Table.
        """
        return self.keys

    @returns(Key)
    def get_key(self):
        return self.get_keys().one()

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Returns:
            The Capabilities instance related to this Table.
        """
        return self.capabilities

    @returns(set)
    def get_names_from_keys(self):
        """
        Returns:
            A set of tuple of Strings.
            - Each tuple corresponds to a Key of this Table.
            - Each String of those tuples corresponds to a
            field name of this Key.
        """
        return set([
            frozenset([
                field.get_name() for field in fields
            ]) for fields in self.get_keys()
        ])

    @returns(set)
    def get_types_from_keys(self):
        """
        Returns:
            A set of frozenset of Strings,
            - Each frozenset corresponds to a Key of 'self'.
            - Each String of those frozenset corresponds to a
            type name involved in this Key.
        """
        return set([
            frozenset([
                field.get_type() for field in fields
            ]) for fields in self.get_keys()
        ])

    @returns(dict)
    def get_partitions(self):
        """
        Returns:
            The dictionnary which maps for each platform its corresponding clause
            (e.g the partition). A None clause means that this clause is always True
        """
        return self.partitions

    def set_partitions(self, partitions):
        self.partitions = dict()
        if isinstance(partitions, (list, set, frozenset)):
            for platform in partitions:
                self.partitions[platform] = None
        elif isinstance(partitions, StringTypes):
            self.partitions[partitions] = None
        elif isinstance(partitions, dict):
            self.partitions = partitions

        self.init_platforms()

    def init_platforms(self):
        self.platforms = set(self.partitions.keys())

    @returns(set)
    def get_platforms(self):
        """
        Returns:
            The set of String where each String is the name of a
            Platform providing this Table.
        """
        return self.platforms

#UNUSED|    #@returns(set) # XXX does not support named arguments
#UNUSED|    def get_fields_with_name(self, names, metadata=None):
#UNUSED|        """
#UNUSED|        \brief Retrieve a set of Field according to their name
#UNUSED|        \param names The name of the requested fields (String instances)
#UNUSED|        \return The set of Field instances nested in this table having a name
#UNUSED|            in names.
#UNUSED|        """
#UNUSED|        fields = set()
#UNUSED|        for field in self.get_fields():
#UNUSED|            #if metadata and field.is_reference():
#UNUSED|            #    key = metadata.find_node(field.get_name()).get_keys().one()
#UNUSED|            #    for key_field in key:
#UNUSED|            #        if key_field.get_name() in names:
#UNUSED|            #            fields.add(field)
#UNUSED|            #elif field.get_name() in names:
#UNUSED|            if field.get_name() in names:
#UNUSED|                fields.add(field)
#UNUSED|        return fields
#UNUSED|
#UNUSED|    @staticmethod
#UNUSED|    #@returns(Table)
#UNUSED|    #@accepts(Table, set)
#UNUSED|    def make_table_from_fields(u, relevant_fields):
#UNUSED|        """
#UNUSED|        \brief Build a sub Table according to a given u Table such as
#UNUSED|            - each field of the sub Table is in relevant_fields
#UNUSED|            - each key only involve Fields of relevant_fields
#UNUSED|        \param u A Table instance
#UNUSED|        \param relevant_fields A set of field names belonging to table
#UNUSED|        \return The corresponding subtable
#UNUSED|        """
#UNUSED|        copy_u = deepcopy(u)
#UNUSED|
#UNUSED|        for field in u.get_fields():
#UNUSED|            if field.get_name() not in relevant_fields:
#UNUSED|                copy_u.erase_field(field.get_name())
#UNUSED|
#UNUSED|        # In copy_u, Key instances refer to Field instance of u, which is
#UNUSED|        # wrong. We've to rebuild properly the relevant keys based on
#UNUSED|        # Field instances of copy_u.
#UNUSED|        copy_u.erase_keys()
#UNUSED|
#UNUSED|        # We rebuild each Key having a sense in the reduced Table,
#UNUSED|        # e.g. those involving only remaining Fields
#UNUSED|        for key in u.get_keys():
#UNUSED|            # We don't always have a key
#UNUSED|            # eg in a pruned tree, we do not need a key for the root unless we want to remove duplicates
#UNUSED|            if set(key) <= relevant_fields:
#UNUSED|                key_copy = set()
#UNUSED|                for field in key:
#UNUSED|                    key_copy.add(copy_u.get_field(field.get_name()))
#UNUSED|                copy_u.insert_key(Key(key_copy))
#UNUSED|
#UNUSED|        # We need to update map_method_fields
#UNUSED|        for method, fields in copy_u.map_method_fields.items():
#UNUSED|            fields &= relevant_fields
#UNUSED|
#UNUSED|        return copy_u
#UNUSED|
#DEPRECATED|    @staticmethod
#DEPRECATED|    #@returns(Table)
#DEPRECATED|    #@accepts(Table, StringTypes)
#DEPRECATED|    def make_table_from_platform(table, fields, platform):
#DEPRECATED|        """
#DEPRECATED|        \brief Extract from a Table instance its Key(s) and Field(s)
#DEPRECATED|            related to a given platform name to make another Table
#DEPRECATED|            instance related to this platform
#DEPRECATED|        \param table A Table instance
#DEPRECATED|        \param fields A set of Fields (those we're extracting)
#DEPRECATED|        \param platform A String value (the name of the platform)
#DEPRECATED|        \return The corresponding Table
#DEPRECATED|        """
#DEPRECATED|        # Extract the corresponding subtable
#DEPRECATED|        ret = Table.make_table_from_fields(table, fields)
#DEPRECATED|
#DEPRECATED|        # Compute the new map_field_method
#DEPRECATED|        updated_map_field_methods = dict()
#DEPRECATED|        for field, methods in table.map_field_methods.items():
#DEPRECATED|            if field in fields:
#DEPRECATED|                for method in methods:
#DEPRECATED|                    if platform == method.get_platform():
#DEPRECATED|                        updated_map_field_methods[field] = set([method])
#DEPRECATED|                        break
#DEPRECATED|                assert len(updated_map_field_methods[field]) != 0, "No method related to field %r and platform %r" % (field, platform)
#DEPRECATED|
#DEPRECATED|        ret.map_field_methods = updated_map_field_methods
#DEPRECATED|        ret.platforms = set([platform])
#DEPRECATED|        return ret

    @returns(dict)
    def get_annotation(self):
        """
        Returns:
            A dictionnary which map for each Method (e.g. platform name +
            method name) the set of Field that can be retrieved
        """
        try:
            # This is created by dbnorm and used by ExploreTask
            # (Table deduced from several Announces having common Keys and Fields)
            return self.map_method_fields
        except AttributeError:
            # ... Otherwise, we can craft it on the fly
            table_name  = self.get_name()
            field_names = self.get_field_names()
            for platform_name in self.get_platforms():
                return {Method(platform_name, table_name): field_names}

    #-----------------------------------------------------------------------
    # Relations between two Table instances
    # Notations in documentation:
    #   u == self  : the source node
    #   v == table : the target node
    #   x.n        : the name of node x
    #   x.k        : the keys of node x
    #   x.f        : a field of node x
    #   x.f.n      : the field name of the field f of node x
    #   x.f.t      : the field type of the field f of node x
    #-----------------------------------------------------------------------


#DEPRECATED|    #@returns(set)
#DEPRECATED|    def get_connecting_fields(self, table):
#DEPRECATED|        """
#DEPRECATED|        \brief Find fields verifying:
#DEPRECATED|            exists f | u.f.t == v.n (P1)
#DEPRECATED|            u = self
#DEPRECATED|            v = table
#DEPRECATED|        \param table The target candidate table
#DEPRECATED|        \return The set of Field f verifying (P1)
#DEPRECATED|        """
#DEPRECATED|        # For now we will suppose a single connecting field
#DEPRECATED|        #connecting_fields = set()
#DEPRECATED|        for field in self.get_fields():
#DEPRECATED|            if field.get_type() == table.get_name():
#DEPRECATED|                return field
#DEPRECATED|        return None
#DEPRECATED|                #connecting_fields.add(field)
#DEPRECATED|        #return connecting_fields
#DEPRECATED|
#DEPRECATED|    @returns(bool)
#DEPRECATED|    def inherits(self, table):
#DEPRECATED|        """
#DEPRECATED|        \brief (Internal use, since this function is called in a specific context)
#DEPRECATED|            Test whether self inherits table
#DEPRECATED|            Example: tophat::destination ==> {tophat, sonoma}::ip
#DEPRECATED|        \param table The target candidate table
#DEPRECATED|        \return True iif u --> v
#DEPRECATED|        """
#DEPRECATED|        name = set()
#DEPRECATED|        name.add(table.get_name())
#DEPRECATED|        return frozenset(name) in table.get_names_from_keys()
#DEPRECATED|
#DEPRECATED|    def get_connecting_fields_jordan(self, table):
#DEPRECATED|        # Does u has a field or a set of fields that are keys in v
#DEPRECATED|        # XXX wrong content and wront name
#DEPRECATED|        u, v = self, table
#DEPRECATED|        for v_key in v.keys(): # MAYBE A SINGLE KEY ?
#DEPRECATED|            if v_key <= u.fields:
#DEPRECATED|                return (u.fields.intersection(v_key), v_key)
#DEPRECATED|        return None

    @returns(bool)
    def is_child_of(self, table):
        """
        Tests whether this Table inherits a given Table.
        Args:
            table: A Table instance.
        Returns:
            True iif this Table inherits 'table'.
        """
        u = self
        v = table
        try:
            return u.keys.one().get_type() == v.keys.one().get_type() and u.get_name() == u.keys.one().get_field_name()
        except:
            return False

    @returns(set)
    def get_relations(self, table):
        """
        Compute which Relations connect the "self" Table (source node) to the
        "table" Table (target node). We assume that the graph of table is
        at least 2nf.
        Args:
            table: The target table
        Returns:
            A set of Relation instances connecting "self" and "table".
            This set is empty iif the both Tables are unrelated.
        """
        # We only test relations u --> v
        u = self
        v = table

        relations = set()

        u_key = u.keys.one()
        v_key = v.keys.one()

        if u.get_name() == v.get_name():
            if u_key.is_composite() or v_key.is_composite():
                Log.warning("@jordan; this allows to run manifold-router, but it should also works for non-composite keys, which is not the case")
                if u_key.is_composite() != v_key.is_composite():
                    Log.warning("strange, only one of those keys is composite: u_key == %s  v_key = %s" % (u_key, v_key))
                p = Predicate(
                    tuple(sorted(u_key.get_field_names())),
                    eq,
                    tuple(sorted(v_key.get_field_names()))
                )
            else:
                p = Predicate(u_key.get_name(), eq, v_key.get_name())

            if u.get_platforms() > v.get_platforms():
                relations.add(Relation(Relation.types.PARENT, p))
            #else:
            #    relations.add(Relation(Relation.types.CHILD, p))
            return relations

        # Detect explicit Relation from u to v
        for field in u.get_fields():
            # 1. A field in u is explicitly typed againt v name
            if field.get_type() == v.get_name():
                if v_key.is_composite():
                    # We assume that u (for ex: traceroute) provides in the current field (ex: hops)
                    # a record containing at least the v's key (for ex: (agent, destination, first, ttl))
                    intersecting_fields = tuple(u.get_field_names() & v_key.get_field_names())
                    p = Predicate(intersecting_fields, eq, intersecting_fields)
                else:
                    p = Predicate(field.get_name(), eq, v_key.get_name())

                if field.is_array():
                    relations.add(Relation(Relation.types.LINK_1N, p, name=field.get_name(), local = v.keys.is_local())) # LINK_1N_FORWARD
                else:
                    if False: # field == key
                        relations.add(Relation(Relation.types.PARENT, p, name=field.get_name())) # in which direction ?????
                    else:
                        if field.is_local():
                            relations.add(Relation(Relation.types.LINK_11, p, name=field.get_name()))
                        else:
                            if v.is_child_of(u):
                                relations.add(Relation(Relation.types.CHILD, p, name = str(uuid.uuid4))) # XXX
                            elif u.is_child_of(v):
                                 relations.add(Relation(Relation.types.PARENT, p, name = str(uuid.uuid4))) # XXX
                            else:
                                if field.get_name() in ['source', 'destination', 'agent', 'dns_target']:
                                    Log.warning("Hardcoded source, agent, destination and dns_target as 1..1 relationships")
                                    relations.add(Relation(Relation.types.LINK_11, p, name=field.get_name()))
                                else:
                                    relations.add(Relation(Relation.types.LINK, p, name = str(uuid.uuid4))) # XXX 
            # BAD
            #if v_key.is_composite():
            #    Log.warning("Link (2) unsupported between u=%s and v=%s: v has a composite key" % (u.get_name(), v.get_name()))
            ## 2. A field is typed like the key
            #if field.get_type() == v_key.get_field().get_type():
            #    # In this case we might have inheritance
            #    # We should point to the toplevel class, ie. if key field type == platform name
            #    # We are back to the previous case.
            #    # a child class is an instance of the parent class, no it should be ok

            # (3) A field point to part of v key (v is thus composite)

            if field.get_type() not in BASE_TYPES and set([field.get_type()]) < v_key.get_field_types():
                # What if several possible combinations
                # How to consider inheritance ?
                vfield = [f for f in v_key if f.get_type() == field.get_type()][0]
                p = Predicate(field.get_name(), eq, vfield.get_name())
                relations.add(Relation(Relation.types.LINK_1N, p, name=field.get_name() + "_" + v.get_name())) # LINK_1N_FORWARD ?
                continue


        # Following relations don't involve a single field

        # (4) A bit more complex: u presents the set of fields that make up v key

        # (5) A bit more complex: u presents part of the fields that make up v key

        if relations:
            return relations

        # Detect implicit Relation from u to v
        intersection = u.get_field_names() & v_key.get_field_names()
        if intersection and intersection < v_key.get_field_names():

            intersection = tuple(intersection)
            if len(intersection) == 1:
                intersection = intersection[0]
            p = Predicate(intersection, eq, intersection)

            relations.add(Relation(Relation.types.LINK_1N, p, name=v.get_name())) # LINK_1N_FORWARD # Name ?
            # we don't continue otherwise we will find subsets of this set
            # note: this code might replace following code operating on a single field
            return relations


        # --- REVERSE RELATIONS
        for field in v.get_fields():
            # (6) inv of (1) a field in v points to an existing type
            # we could say we only look at key types at this stage
            if field.get_type() == u.get_name():
                if u_key.is_composite():
                    Log.warning("Link (6) unsupported between u=%s and v=%s: u has a composite key" % (u.get_name(), v.get_name()))
                    continue
                p = Predicate(u_key.get_name(), eq, field.get_name())
                if field.is_array():
                    relations.add(Relation(Relation.types.LINK_1N_BACKWARDS, p, name = v.get_name()))
                    ### was: COLLECTION, p)) # a u is many v ? approve this type
                    #relations.add(Relation(Relation.types.COLLECTION, p)) # a u is many v ? approve this type
                else:
                    # if u parent
                    if v.is_child_of(u):
                        relations.add(Relation(Relation.types.CHILD, p))
                    elif u.is_child_of(v):
                         relations.add(Relation(Relation.types.PARENT, p))
                    else:
                        relations.add(Relation(Relation.types.LINK_1N, p, name=v.get_name())) # LINK_1N_BACKWARDS

        return relations

#DEPRECATED|        # OLD CODE FOLLOWS
#DEPRECATED|
#DEPRECATED|        # XXX This is broken
#DEPRECATED|        #if not u.get_platforms() >= v.get_platforms():
#DEPRECATED|        #    return None
#DEPRECATED|
#DEPRECATED|        connecting_fields = u.get_connecting_fields(v)
#DEPRECATED|        # We temporarity changed the relation to return a single field...
#DEPRECATED|        # 1) FK -> Table.PK
#DEPRECATED|        if connecting_fields:
#DEPRECATED|            # FK --> PK : simple join or view
#DEPRECATED|            if connecting_fields.is_array():
#DEPRECATED|                return (Relation.types.LINK_1N, set([connecting_fields]))
#DEPRECATED|            else:
#DEPRECATED|                return (Relation.types.LINK, set([connecting_fields]))
#DEPRECATED|
#DEPRECATED|        # 2)
#DEPRECATED|        connecting_keys = u.keys.intersection(v.keys)
#DEPRECATED|        if connecting_keys:
#DEPRECATED|            connecting_keys = iter(connecting_keys).next() # pick one
#DEPRECATED|            # u.PK --> v.PK
#DEPRECATED|            if u.get_name() != v.get_name():
#DEPRECATED|                # Different name = inheritance
#DEPRECATED|                # XXX direction ????
#DEPRECATED|                return (Relation.types.INHERITANCE, connecting_keys)
#DEPRECATED|            else:
#DEPRECATED|                if u.get_platforms() >= v.get_platforms():
#DEPRECATED|                    # Specialization = parent tables created during dbnorm
#DEPRECATED|                    # (same name, and full set of platforms)
#DEPRECATED|                    return (Relation.types.SPECIALIZATION, connecting_keys)

    @returns(list)
    def get_invalid_keys(self):
        """
        Returns:
            The list Keys involving at least one field not present in this Table.
        """
        invalid_keys = []
        for key in self.keys:
            key_found = True
            for key_elt in key:
                key_elt_found = False
                for field in self.fields.values():
                    if key_elt == field:#.get_name():
                        key_elt_found = True
                        break
                if key_elt_found == False:
                    key_found = False
                    break
            if key_found == False:
                invalid_keys.append(key)
                break
        return invalid_keys

#UNUSED|    def get_invalid_types(self, valid_types):
#UNUSED|        """
#UNUSED|        \return Types not present in the table
#UNUSED|        """
#UNUSED|        invalid_types = []
#UNUSED|        for field in self.fields:
#UNUSED|            cur_type = field.type
#UNUSED|            if cur_type not in valid_types and cur_type not in BASE_TYPES:
#UNUSED|                print ">> %r: adding invalid type %r (valid_types = %r)" % (self.class_name, cur_type, valid_types)
#UNUSED|                invalid_types.append(cur_type)
#UNUSED|        return invalid_types

    def to_dict(self):
        """
        Returns:
            The dictionnary describing this Table for metadata.
        """
        # Build columns from fields
        columns = list()
        for field in self.fields.values():
            columns.append(field.to_dict())

        keys = tuple(self.get_keys().one().get_field_names())

        return {
            "table"        : self.get_name(),
            "columns"      : columns,
            "key"          : keys,
            "capabilities" : self.get_capabilities().to_list()
            # key
            # default
            # capabilities
        }
