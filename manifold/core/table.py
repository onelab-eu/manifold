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

from copy                   import deepcopy
from types                  import StringTypes

from manifold.core.field      import Field
from manifold.core.filter     import Filter
from manifold.core.key        import Key, Keys 
from manifold.util.type       import returns, accepts 
from manifold.core.method     import Method 

class Table:
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
    def check_init(partitions, map_field_methods, name, fields, keys, cost):
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

    def __init__(self, partitions, map_field_methods, name, fields, keys, cost = 1):
        """
        \brief Constructor
        \param partitions It can be either:
            - a dictionary {String => Predicate} where each key is the name of a platform
            and each data is a Predicate or None. None means that the condition is always True.
            - or either set/list of platform names
        \param name The name of the table (for example: 'user', ...)
        \param map_field_methods Pass None or a dictionnary which maps for each field
            the corresponding methods to retrieve them: {Field => set(Method)}
        \param fields A set/list of Fields involved in the table (for example 'name', 'email', ...)
        \param keys The key of the table (for example 'email')
        \param cost An integer (unused for the moment)
        """
        # Check parameters
        Table.check_init(partitions, map_field_methods, name, fields, keys, cost)

        self.partitions = dict()
        if isinstance(partitions, (list, set, frozenset)):
            for platform in partitions:
                self.partitions[platform] = None
        elif isinstance(partitions, StringTypes):
            self.partitions[partitions] = None
        elif isinstance(partitions, dict):
            self.partitions = partitions 

        # Init self.fields
        self.fields = dict()
        if isinstance(fields, (list, set, frozenset)):
            for field in fields:
                self.insert_field(field)
        elif isinstance(fields, dict):
            self.fields = fields
        
        # Init self.keys
        self.keys = Keys()
        for key in keys:
            self.insert_key(key)

        # Init self.platforms
        self.platforms = set(self.partitions.keys())
        if isinstance(map_field_methods, dict):
            for methods in map_field_methods.values():
                for method in methods:
                    self.platforms.add(method.get_platform())
 
        # Other fields
        self.name = name
        self.cost = cost
        self.map_field_methods = map_field_methods

    #-----------------------------------------------------------------------
    # Outputs 
    #-----------------------------------------------------------------------

    @returns(unicode)
    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "{%s}::%s {\n\t%s\n\n\t%s;\n};" % (
            ', '.join([p          for p in sorted(self.get_platforms())]),
            self.get_name(),
#            ';\n\t'.join(["%s" % f for f in sorted(self.get_fields())]),
            '\n\t'.join(["%s;\t// via %r" % (field, methods) for field, methods in self.map_field_methods.items()]),
            ';\n\t'.join(["%s" % k for k in self.get_keys()])
        )

    @returns(unicode)
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

    @returns(str)
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
            A Field      (a Field instance belonging to this table)
            A StringType (a field name, related to a field of this table)
            A container  (list, set, frozenset, tuple) made of StringType (field names)
        """
        if isinstance(key, Key):
            self.keys.add(key)
        else:
            if isinstance(key, Field):
                fields = frozenset(key)
            elif isinstance(key, StringTypes):
                fields = frozenset(self.get_field(key))
            elif isinstance(key, (list, set, frozenset, tuple)):
                fields = frozenset([self.get_field(key_elt) for key_elt in key])
            else:
                raise TypeError("key = %r is not of type Key nor Field nor StringTypes")
            self.keys.add(Key(fields))
 
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

    @returns(set)
    def get_platforms(self):
        """
        \return The set of platform that corresponds to this table
        """
        return self.platforms

    @returns(set)
    def get_fields_with_name(self, names):
        """
        \brief Retrieve a set of Field according to their name
        \param names The name of the requested fields (String instances)
        \return The set of Field instances nested in this table having a name
            in names.
        """
        fields = set()
        for field in self.get_fields():
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
        \param relevant_fields A set of Fields belonging to table
        \return The corresponding subtable
        """
        copy_u = deepcopy(u)

        for field in u.get_fields():
            if field not in relevant_fields:
                copy_u.erase_field(field.get_name())

        # In copy_u, Key instances refer to Field instance of u, which is
        # wrong. We've to rebuild properly the relevant keys based on
        # Field instances of copy_u.
        copy_u.erase_keys()

        # We rebuild each Key having a sense in the reduced Table,
        # e.g. those involving only remaining Fields
        for key in u.get_keys():
            if set(key) <= relevant_fields:
                key_copy = set()
                for field in key:
                    key_copy.add(copy_u.get_field(field.get_name()))
                copy_u.insert_key(Key(key_copy))

        return copy_u

    @staticmethod
    #@returns(Table)
    #@accepts(Table, str)
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
        map_method_fields = dict()
        for field, methods in self.map_field_methods.items():
            for method in methods:
                if method not in map_method_fields.keys():
                    map_method_fields[method] = set()
                map_method_fields[method].add(field)
        return map_method_fields 

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

#OBSOLETE|    @returns(set)
#OBSOLETE|    def get_fields_in_key(self, fields):
#OBSOLETE|        """
#OBSOLETE|        \brief Compute which fields belong to a single-key of "self" Table.
#OBSOLETE|        \param fields A set of Field
#OBSOLETE|        \return A set of MetaField (that may be empty) included in "fields"
#OBSOLETE|        """
#OBSOLETE|        fields_in_key = set()
#OBSOLETE|        for field in fields: 
#OBSOLETE|            for key in self.keys:
#OBSOLETE|                if key.is_composite():
#OBSOLETE|                    continue
#OBSOLETE|                if field.type == key.get_type():
#OBSOLETE|                    fields_in_key.add(field) 
#OBSOLETE|        return fields_in_key 

    @returns(set)
    def get_connecting_fields(self, table):
        """
        \brief Find fields verifying: 
            exists f | u.f.t == v.n (P1)
            u = self
            v = table
        \param table The target candidate table
        \return The set of Field f verifying (P1) 
        """
        connecting_fields = set()
        for field in self.get_fields():
            if field.get_type() == table.get_name():
                connecting_fields.add(field)
        return connecting_fields 

    @returns(Keys)
    def get_connecting_keys(self, fields):
        """
        \brief Find Key(s) of self such has k \subseteq fields
        \param fields A set of Field instances
        \return The corresponding Keys (set of Key) instance
        """
        connecting_keys = Keys()
        for key in self.get_keys():
            if key <= fields:
                connecting_keys.add(key)
        return connecting_keys

#OBSOLETE|    @returns(bool)
#OBSOLETE|    def includes(self, table):
#OBSOLETE|        """
#OBSOLETE|        \brief (Internal use, since this function is called in a specific context)
#OBSOLETE|            Test whether self and table have the same table name.
#OBSOLETE|            u ==> v iif u.n == v.n
#OBSOLETE|            Example: tophat::ip ==> {tophat, sonoma}::ip
#OBSOLETE|        \param table The target candidate table
#OBSOLETE|        \return True iif u ==> v
#OBSOLETE|        """
#OBSOLETE|        return self.get_name() == table.get_name()

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

#OBSOLETE|    @returns(bool)
#OBSOLETE|    def has_intersecting_keys(self, fields):
#OBSOLETE|        return fields in self.get_keys():

    def get_relation(self, table):
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
        u = self
        v = table
        connecting_fields_uv = u.get_connecting_fields(v)
        if connecting_fields_uv != set():
            connecting_keys_uv = u.get_connecting_keys(connecting_fields_uv)
            if connecting_keys_uv == set():
                return ("~~>", connecting_fields_uv)
#OBSOLETE|            elif u.includes(v):
#OBSOLETE|                return ("==>", None)
            elif u.inherits(v):
                return ("-->", connecting_keys_uv) 
        return None

