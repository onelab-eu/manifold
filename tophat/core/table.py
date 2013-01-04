#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Table:
# Stores the representation of a table in the 3-nf graph
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from tophat.metadata.MetadataField import MetadataField
from tophat.core.filter            import Filter
from tophat.core.key               import Key, Keys 
from types                         import StringTypes
from tophat.util.type              import returns, accepts 

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
                if not isinstance(field, MetadataField):
                    raise TypeError("In fields = %r: %r is not of type MetadataField" % (fields, field))
        elif isinstance(fields, dict):
            for field_name, metafield in fields.items():
                if not isinstance(field_name, StringTypes):
                    raise TypeError("In fields = %r: %r is not of type StringTypes" % (fields, field_name))
                elif not isinstance(metafield, MetadataField):
                    raise TypeError("In fields = %r: %r is not of type MetadataField" % (fields, metafield))

    @staticmethod
    def check_keys(keys):
        """
        \brief Check whether keys parameter is well-formed in __init__
        """
        if not isinstance(keys, (tuple, list, frozenset)):
            raise TypeError("keys = %r is not of type tuple, list, frozenset" % keys) 
        for key in keys:
            if isinstance(key, (tuple, list, frozenset)):
                for key_elt in key:
                    if not isinstance(key_elt, StringTypes):
                        raise TypeError("In key %r: %r is not of type StringTypes" % (key, key_elt))
            elif not isinstance(key, StringTypes):
                raise TypeError("In key = %r: %r is not of type StringTypes" % key)

    @staticmethod
    @returns(bool)
    def check_partitions(partitions):
        """
        \brief Check whether keys partitions is well-formed in __init__
        \return True iif keys is well-formed
        """
        if isinstance(partitions, (list, set, frozenset)):
            # partitions carries a set of platforms
            for platform in partitions:
                if not isinstance(platform, StringTypes):
                   raise TypeError("In partitons = %r: platform = %r is not of type StringTypes" % (partitions, platform)) 
        elif isinstance(partitions, StringTypes):
            return True
        elif isinstance(partitions, dict):
            for platforms, clause in partitions.items():
                if platforms and not isinstance(platforms, frozenset):
                    return TypeError("platforms = %r is not of type frozenset" % platforms) 
                if clause and not isinstance(clause, Predicate):
                    return TypeError("clause = %r is not of type Predicate" % clause) 
        return True 

    #-----------------------------------------------------------------------
    # Constructor 
    #-----------------------------------------------------------------------

    def __init__(self, partitions, name, fields, keys, cost = 1):
        """
        \brief Constructor
        \param partitions A dictionary which indicates for each platform the corresponding
            predicate (pass None if not Predicate needed e.g. always True).
        \param name The name of the table (for example: 'user', ...)
        \param fields The fields involved in the table (for example 'name', 'email', ...)
        \param keys The key of the table (for example 'email')
        \param cost
        """
        # Check parameters
        Table.check_fields(fields)
        Table.check_keys(keys)
        Table.check_partitions(partitions)

        # self.partitions
        self.partitions = dict()
        if isinstance(partitions, (list, set, frozenset)):
            for platform in partitions:
                self.partitions[platform] = None
        elif isinstance(partitions, StringTypes):
            self.partitions[partitions] = None
        elif isinstance(partitions, dict):
            self.partitions = partitions 
        self.name = name

        # self.fields
        self.fields = dict()
        if isinstance(fields, (list, set, frozenset)):
            for field in fields:
                self.insert_field(field)
        elif isinstance(fields, dict):
            self.fields = fields

        #self.keys = to_frozenset(keys)
        self.keys = Keys()
        for key in keys:
            self.insert_key(key)
        self.cost = cost
        # TODO There will also be a list that the platform cannot provide, cf sources[i].fields

       
    #-----------------------------------------------------------------------
    # Outputs 
    #-----------------------------------------------------------------------

    @returns(str)
    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "<{%s}::%s fields = {%s} keys = %r>" % (
            ', '.join([p            for p in sorted(self.get_platforms())]),
            self.get_name(),
            ', '.join([f.get_name() for f in sorted(self.get_fields())]),
            self.keys
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
    def is_key(self, key):
        """
        \brief Test whether a field is a key of this table
        \param key The name of the field (StringTypes or MetadataField).
            You might pass an tuple or a list of fields (StringTypes or MetadataField)
            if your testing a composite key.
        \return True iif only this is a key
        """
        if isinstance(key, (list, set, frozenset)):
            key = tuple(key)
        elif isinstance(key, (StringTypes, MetadataField)):
            key = (key,)
        elif not isinstance(key, tuple):
            raise TypeError("is_key: %s must be a list, a tuple, a set, a frozenset or a string" % key)
        key = set([k if isinstance(k, StringTypes) else k.get_name() for k in key])
        return key in self.get_names_from_keys()

    def insert_field(self, field):
        """
        \brief Add a field in self.
        \param field A MetadataField instance 
        """
        self.fields[field.get_name()] = field
 
    def get_fields(self):
        """
        \return the MetadataField instances related to this table 
        """
        return self.fields.values()

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
            A Metafield  (a field belonging to this table)
            A StringType (a field name, related to a field of this table)
            A container (list, set, frozenset, tuple) made of StringType (field names)
        """
        if isinstance(key, Key):
            self.keys.add(key)
        else:
            if isinstance(key, MetadataField):
                fields = frozenset(key)
            elif isinstance(key, StringTypes):
                fields = frozenset(self.get_field(key))
            elif isinstance(key, (list, set, frozenset, tuple)):
                fields = frozenset([self.get_field(key_elt) for key_elt in key])
            else:
                raise TypeError("key = %r is not of type Key nor MetadataField nor StringTypes")
            self.keys.add(Key(fields))
 
    def get_field(self, field_name):
        """
        \brief Retrieve a MetadataField instance stored in self according to
            its field name.
        \param field_name The name of the requested field.
        \return The MetadataField instance identified by 'field_name'
        """
        # Retrieve the field name
        if isinstance(field_name, MetadataField):
            field_name = field_name.get_name()
        elif not isinstance(field_name, StringTypes):
            raise TypeError("get_field: '%s' has an invalid type (%s): supported types are StringTypes and MetadataField" % (field_name, type(field_name)))

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
    def get_fields_from_keys(self):
        """
        \return A set of tuple of MetadataField.
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a MetadataField.
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
            ]) for fields in self.get_fields_from_keys()
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
            ]) for fields in self.get_fields_from_keys()
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
        return set(self.partitions.keys())

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

    @returns(set)
    def get_fields_in_key(self, fields):
        """
        \brief Compute which fields belong to a single-key of "self" Table.
        \param fields A set of MetadataField
        \return A set of MetaField (that may be empty) included in "fields"
        """
        fields_in_key = set()
        for field in fields: 
            for key in self.keys:
                if key.is_composite():
                    continue
                if field.type == key.get_type():
                    fields_in_key.add(field) 
        return fields_in_key 

    @returns(set)
    def get_connecting_fields(self, table):
        """
        \brief Find fields verifying: 
            exists f | u.f.t == v.n or u.f.n == v.n (P1)
        \param table The target candidate table
        \return The set of MetadataField f verifying (P1) 
        """
        connecting_fields = set()
        for field in self.get_fields():
            if field.get_name() == table.get_name() or field.get_type() == table.get_name():
                connecting_fields.add(field)
        return connecting_fields 

    @returns(bool)
    def includes(self, table):
        """
        \brief (Internal use, since this function is called in a specific context)
            Test whether self and table have the same table name.
            u ==> v iif u.n == v.n
            Example: tophat::ip ==> {tophat, sonoma}::ip
        \param table The target candidate table
        \return True iif u ==> v
        """
        return self.get_name() == table.get_name()

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

    @returns(bool)
    def has_intersecting_key(self, fields, debug):
        """
        \brief Test whether a set of fields intersect at least one (single) key
        \param fields A set of MetadataField instances
        \return True iif at least one intersecting key exists, False otherwise 
        """
        return fields in self.get_fields_from_keys()

    def get_relation(self, table):
        """
        \brief Compute which kind of relation connects
            the "self" Table (source node) to the "table"
            Table (target node). We assume that the graph
            of table is at least 2nf.
            \sa tophat/util/dbgraph.py
        \param table The target table
        \return
            - None if the both tables are unrelated
            - Otherwise, a tuple made of
                - a string: "==>", "-->", "~~>"
                - a set of MetadataField that will be stored in the arc 
        """
        u = self
        v = table
        connecting_fields_uv = u.get_connecting_fields(v)
        debug = (u.get_name() == "traceroute" and u.get_platforms() == set(["sonoma"]) and v.get_name() == "ip")
        if connecting_fields_uv != set():
            connecting_fields_vu = v.get_connecting_fields(u)
            if not u.has_intersecting_key(connecting_fields_uv, debug):
                return ("~~>", connecting_fields_uv)
            elif u.includes(v):
                # Patch: avoid to link tophat::ip ==> sonoma::ip
                if u.get_platforms() <= v.get_platforms():
                    return ("==>", None)
            elif u.inherits(v):
                return ("-->", v.get_fields_in_key(connecting_fields_uv)) 
        return None
