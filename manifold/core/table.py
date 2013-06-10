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

from manifold.core.field        import Field
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
        \brief Check whether fields parameter is well-formed in __init__
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
        \brief Check whether keys parameter is well-formed in __init__
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
        \brief Check whether keys partitions is well-formed in __init__
        \return True iif keys is well-formed
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
    def check_map_field_methods(map_field_methods):
        if map_field_methods == None:
            return
        if not isinstance(map_field_methods, dict):
            raise TypeError("Invalid map_field_methods = %r (%r)" % (map_field_methods, type(map_field_methods)))
        for field, methods in map_field_methods.items():
            if not isinstance(field, Field):
                raise TypeError("Invalid field = %r (%r) in %r" % (field, type(field), map_field_methods))
            for method in methods:
                if not isinstance(method, Method):
                    raise TypeError("Invalid method = %r (%r) in %r" % (method, type(method), map_field_methods))

    @staticmethod
    def check_init(partitions, map_field_methods, name, fields, keys):
        """
        \brief Check whether parameters passed to __init__ are well formed
        """
        Table.check_fields(fields)
        Table.check_keys(keys)
        Table.check_map_field_methods(map_field_methods)
        Table.check_partitions(partitions)
        if map_field_methods:
            if set(map_field_methods.keys()) != set(fields):
                raise ValueError("Incoherent parameters: %r must be equal to %r" % (map_field_methods.keys(), fields))

    #-----------------------------------------------------------------------
    # Constructor 
    #-----------------------------------------------------------------------

    def __init__(self, partitions, map_field_methods, name, fields, keys):
        """
        Table constructor
        Args:
            partitions: It can be either:
                - a string (the name of the platform)
                - a dictionary {String => Predicate} where each key is the name
                of a platform and each data is a Predicate or "None". "None" means
                that the condition is always True.
                - a set/list of platform names
            name: The name of the table
            map_field_methods: Pass "None" or a dictionnary which maps for each field
                the corresponding methods to retrieve them: {Field => set(Method)}
            fields: A set/list of Fields involved in the table
            keys: A set of Key instances
        """
        # Check parameters
        Table.check_init(partitions, map_field_methods, name, fields, keys)
        self.set_partitions(partitions)

        # Init self.fields
        self.fields = dict()
        if isinstance(fields, (list, set, frozenset)):
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
        if isinstance(map_field_methods, dict):
            for methods in map_field_methods.values():
                for method in methods:
                    self.platforms.add(method.get_platform())

        # Init default capabilities (none)
        self.capabilities = Capabilities()
 
        # Other fields
        self.name = name
        self.map_field_methods = map_field_methods if map_field_methods else {}

    #-----------------------------------------------------------------------
    # Outputs 
    #-----------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "{%s}::%s {\n\t%s;\n\t%s;\n};" % (
            ', '.join([p          for p in sorted(self.get_platforms())]),
            self.get_name(),
            ';\n\t'.join(["%s%s" % (f, "[]" if f.is_array() else "") for f in sorted(self.get_fields())]),
#            '\n\t'.join(["%s;\t// via %r" % (field, methods) for field, methods in self.map_field_methods.items()]),
            '\n\t;'.join(["%s" % k for k in self.get_keys()])
        )

    @returns(StringTypes)
    def __repr__(self):
        """
        \brief Convert a Table instance into a string ('%r')
        \return The corresponding string
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
        \return the table name of self
        """
        return self.name

    @returns(bool)
    def has_key(self, key):
        """
        \brief Test whether a field is a key of this table
        \param key A Key instance 
        \return True iif only this is a key
        """
        if not isinstance(key, Key):
            raise TypeError("Invalid key = %r (%r)", (key, type(key)))
        return key in self.keys

    def insert_field(self, field):
        """
        \brief Add a field in self.
        \param field A Field instance 
        """
        self.fields[field.get_name()] = field
 
    @returns(set)
    def get_fields(self):
        """
        \return the Field instances related to this table 
        """
        return set(self.fields.values())

    def get_field(self, field_name):
        return self.fields[field_name]

    def get_field_type(self, field_name):
        return self.get_field(field_name).get_type()

    @returns(bool)
    def erase_field(self, field_name):
        """
        \brief Remove a field from the table
        \param field_name The name of the field we remove
        \return True iif the field has been successfully removed
        """
        ret = field_name in self.fields
        del(self.fields[field_name])
        return ret

    def insert_key(self, key):
        """
        \brief Add a field in self.
        \param key Supported parameters
            A Key
            A Field      (a Field instance belonging to this table)
            A StringType (a field name, related to a field of this table)
            A container  (list, set, frozenset, tuple) made of StringType (field names)
        """
        if isinstance(key, Key):
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
            self.keys.add(Key(fields))

    def set_capability(self, capability):
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
        \brief Remove a Key for this table 
        \param key A Key instance.
            \warning This Key must refer Field instance(s) of this table. 
        \return True iif the Key has been found and successfully removed 
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
        \brief Remove every Key for this table
        """
        self.keys = Keys()

    def insert_methods(self, methods):
        """
        \brief Add a pseudo method for every field of the table
        \param method A Method instance
        """
        # update self.map_field_methods
        for field in self.map_field_methods.keys():
            key = Key([field])
            if not self.has_key(key):
                self.map_field_methods[field] |= methods

        # update self.platforms
        for method in methods:
            self.platforms.add(method.get_platform())

    def get_field(self, field_name):
        """
        \brief Retrieve a Field instance stored in self according to
            its field name.
        \param field_name The name of the requested field (String or Field instance).
        \return The Field instance identified by 'field_name'
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
        \brief Retrieve the field names of the fields stored in self.
        \return A set of strings (one string per field name) if several fields
        """
        return set(self.fields.keys())

    @returns(Keys)
    def get_keys(self):
        """
        \return A set of tuple of Field.
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a Field.
        """
        return self.keys

    @returns(Capabilities)
    def get_capabilities(self):
        """
        \return A Capabilities object
        """
        return self.capabilities

    @returns(set)
    def get_names_from_keys(self):
        """
        \return A set of tuple of field names
            Each tuple corresponds to a key of 'self'.
            Each element of these tuples is a String.
        """
        return set([
            frozenset([
                field.get_name() for field in fields
            ]) for fields in self.get_keys()
        ]) 

    @returns(set)
    def get_types_from_keys(self):
        """
        \return A set of tuple of types 
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a typename 
        """

        return set([
            frozenset([
                field.get_type() for field in fields
            ]) for fields in self.get_keys()
        ]) 

    @returns(dict)
    def get_partitions(self):
        """
        \return The dictionnary which map for each platform its corresponding clause
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
        \return The set of platform that corresponds to this table
        """
        return self.platforms

    #@returns(set) # XXX does not support named arguments
    def get_fields_with_name(self, names, metadata=None):
        """
        \brief Retrieve a set of Field according to their name
        \param names The name of the requested fields (String instances)
        \return The set of Field instances nested in this table having a name
            in names.
        """
        fields = set()
        for field in self.get_fields():
            #if metadata and field.is_reference():
            #    key = metadata.find_node(field.get_name()).get_keys().one()
            #    for key_field in key:
            #        if key_field.get_name() in names:
            #            fields.add(field)
            #elif field.get_name() in names:
            if field.get_name() in names:
                fields.add(field)
        return fields

    @staticmethod
    #@returns(Table)
    #@accepts(Table, set)
    def make_table_from_fields(u, relevant_fields):
        """
        \brief Build a sub Table according to a given u Table such as
            - each field of the sub Table is in relevant_fields
            - each key only involve Fields of relevant_fields
        \param u A Table instance
        \param relevant_fields A set of field names belonging to table
        \return The corresponding subtable
        """
        copy_u = deepcopy(u)

        for field in u.get_fields():
            if field.get_name() not in relevant_fields:
                copy_u.erase_field(field.get_name())

        # In copy_u, Key instances refer to Field instance of u, which is
        # wrong. We've to rebuild properly the relevant keys based on
        # Field instances of copy_u.
        copy_u.erase_keys()

        # We rebuild each Key having a sense in the reduced Table,
        # e.g. those involving only remaining Fields
        for key in u.get_keys():
            # We don't always have a key
            # eg in a pruned tree, we do not need a key for the root unless we want to remove duplicates
            if set(key) <= relevant_fields:
                key_copy = set()
                for field in key:
                    key_copy.add(copy_u.get_field(field.get_name()))
                copy_u.insert_key(Key(key_copy))

        # We need to update map_method_fields
        for method, fields in copy_u.map_method_fields.items():
            Log.tmp("update map_method", fields, relevant_fields)
            fields &= relevant_fields

        return copy_u

    @staticmethod
    #@returns(Table)
    #@accepts(Table, StringTypes)
    def make_table_from_platform(table, fields, platform):
        """
        \brief Extract from a Table instance its Key(s) and Field(s)
            related to a given platform name to make another Table
            instance related to this platform
        \param table A Table instance
        \param fields A set of Fields (those we're extracting)
        \param platform A String value (the name of the platform)
        \return The corresponding Table
        """
        # Extract the corresponding subtable
        ret = Table.make_table_from_fields(table, fields)

        # Compute the new map_field_method 
        updated_map_field_methods = dict()
        for field, methods in table.map_field_methods.items():
            if field in fields:
                for method in methods:
                    if platform == method.get_platform():
                        updated_map_field_methods[field] = set([method])
                        break
                assert len(updated_map_field_methods[field]) != 0, "No method related to field %r and platform %r" % (field, platform)

        ret.map_field_methods = updated_map_field_methods
        ret.platforms = set([platform])
        return ret

    @returns(dict)
    def get_annotations(self):
        """
        \return A dictionnary which map for each Method (e.g. platform +
            method name) the set of Field that can be retrieved 
        """
        return self.map_method_fields 

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


    #@returns(set)
    def get_connecting_fields(self, table):
        """
        \brief Find fields verifying: 
            exists f | u.f.t == v.n (P1)
            u = self
            v = table
        \param table The target candidate table
        \return The set of Field f verifying (P1) 
        """
        # For now we will suppose a single connecting field
        #connecting_fields = set()
        for field in self.get_fields():
            if field.get_type() == table.get_name():
                return field
        return None
                #connecting_fields.add(field)
        #return connecting_fields 

    @returns(bool)
    def inherits(self, table):
        """
        \brief (Internal use, since this function is called in a specific context)
            Test whether self inherits table
            Example: tophat::destination ==> {tophat, sonoma}::ip
        \param table The target candidate table
        \return True iif u --> v
        """
        name = set()
        name.add(table.get_name())
        return frozenset(name) in table.get_names_from_keys()

    def get_connecting_fields_jordan(self, table):
        # Does u has a field or a set of fields that are keys in v
        # XXX wrong content and wront name
        u, v = self, table
        for v_key in v.keys(): # MAYBE A SINGLE KEY ?
            if v_key <= u.fields:
                return (u.fields.intersection(v_key), v_key)
        return None

    def is_child_of(self, table):
        u = self
        v = table
        try:
            return u.keys.one().get_type() == v.keys.one().get_type() and u.get_name() == u.keys.one().get_field_name()
        except:
            return False

    def get_relations(self, table):
        """
        \brief Compute which kind of relation connects
            the "self" Table (source node) to the "table"
            Table (target node). We assume that the graph
            of table is at least 2nf.
            \sa manifold.core.dbgraph.py
        \param table The target table
        \return
            - None if the both tables are unrelated
            - Otherwise, a tuple made of
                - a string: "==>", "-->", "~~>"
                - a set of Field that will be stored in the arc 
        """
        # We only test relations u --> v
        u = self
        v = table

        relations = set()

        u_key = u.keys.one()
        v_key = v.keys.one()

        if u.get_name() == v.get_name():
            p = Predicate(u_key.get_name(), eq, v_key.get_name())
            if u.get_platforms() > v.get_platforms():
                relations.add(Relation(Relation.types.PARENT, p))
            #else:
            #    relations.add(Relation(Relation.types.CHILD, p))
            return relations

        for field in u.get_fields():
            # 1. A field in u is explicitly typed againt v name
            if field.get_type() == v.get_name():
                if v_key.is_composite():
                    Log.warning("Link (1) unsupported between u=%s and v=%s: v has a composite key" % (u.get_name(), v.get_name()))
                    continue
                p = Predicate(field.get_name(), eq, v_key.get_name())
                if field.is_array():
                    relations.add(Relation(Relation.types.LINK_1N, p, name=field.get_name())) # LINK_1N_FORWARD
                else:
                    if False: # field == key
                        relations.add(Relation(Relation.types.PARENT, p, name=field.get_name())) # in which direction ?????
                    else:
                        if field.is_local():
                            relations.add(Relation(Relation.types.LINK_11, p))
                        else:
                            if v.is_child_of(u):
                                relations.add(Relation(Relation.types.CHILD, p))
                            elif u.is_child_of(v):
                                 relations.add(Relation(Relation.types.PARENT, p))
                            else:
                                if field.get_name() in ['source', 'destination', 'dns_target']:
                                    Log.warning("Hardcoded source, destination and dns_target as 1..1 relationships")
                                    relations.add(Relation(Relation.types.LINK_11, p))
                                else:
                                    relations.add(Relation(Relation.types.LINK, p))

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
                relations.add(Relation(Relation.types.LINK_1N, p, name=field.get_name())) # LINK_1N_FORWARD ?
                continue
        

        # Following relations don't involve a single field

        # (4) A bit more complex: u presents the set of fields that make up v key

        # (5) A bit more complex: u presents part of the fields that make up v key

        if relations:
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

        # OLD CODE FOLLOWS

        # XXX This is broken
        #if not u.get_platforms() >= v.get_platforms():
        #    return None

        connecting_fields = u.get_connecting_fields(v)
        # We temporarity changed the relation to return a single field...
        # 1) FK -> Table.PK
        if connecting_fields:
            # FK --> PK : simple join or view
            if connecting_fields.is_array():
                return (Relation.types.LINK_1N, set([connecting_fields]))
            else:
                return (Relation.types.LINK, set([connecting_fields]))

        # 2) 
        connecting_keys = u.keys.intersection(v.keys)
        if connecting_keys:
            connecting_keys = iter(connecting_keys).next() # pick one
            # u.PK --> v.PK
            if u.get_name() != v.get_name():
                # Different name = inheritance
                # XXX direction ????
                return (Relation.types.INHERITANCE, connecting_keys)
            else:
                if u.get_platforms() >= v.get_platforms():
                    # Specialization = parent tables created during dbnorm
                    # (same name, and full set of platforms)
                    return (Relation.types.SPECIALIZATION, connecting_keys)
                    
    def get_invalid_keys(self):
        """
        \return The keys that involving one or more field not present in the table
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

    def get_invalid_types(self, valid_types):
        """
        \return Types not present in the table
        """
        invalid_types = []
        for field in self.fields:
            cur_type = field.type
            if cur_type not in valid_types and cur_type not in BASE_TYPES: 
                print ">> %r: adding invalid type %r (valid_types = %r)" % (self.class_name, cur_type, valid_types)
                invalid_types.append(cur_type)
        return invalid_types
