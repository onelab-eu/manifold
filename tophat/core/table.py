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
from types                         import StringTypes
from tophat.util.type              import returns, accepts 

def to_frozenset(t):
    """
    \brief Convert an list of lists into a frozen set of tuples
        ex: [['a', 'b'], ['c', 'd', 'e']] becomes {('a', 'b'), ('c', 'd', 'e')}
    \param t The list of lists
    \return The corresponding frozenset of tuples
    """
    if isinstance(t, list):
        return frozenset([tuple(x) if isinstance(x, (tuple, list)) else x for x in t])
    raise TypeError("to_frozenset: invalid parameter %r (list expected)" % t)

class Table:
    """
    Implements a database table schema.
    """
    
    #-----------------------------------------------------------------------
    # Internal usage
    #-----------------------------------------------------------------------

    @staticmethod
    @returns(bool)
    def check_fields(fields):
        """
        \brief Check whether fields parameter is well-formed in __init__
        \return True iif fields is well-formed
        """
        if fields == None:
            return False
        elif isinstance(fields, (set, frozenset, list, tuple)):
            for field in fields:
                if not isinstance(field, MetadataField):
                    return False
        elif isinstance(fields, dict):
            for field_name, metafield in fields.items():
                if not isinstance(field_name, StringTypes):
                    return False
                elif not isinstance(metafield, MetadataField):
                    return False
        return True

    @staticmethod
    @returns(bool)
    def check_keys(keys):
        """
        \brief Check whether keys parameter is well-formed in __init__
        \return True iif keys is well-formed
        """
        if not isinstance(keys, (tuple, list, frozenset)):
            return False
        for key in keys:
            if isinstance(key, (tuple, list, frozenset)):
                for key_elt in key:
                    if not isinstance(key_elt, StringTypes):
                        return False 
            elif not isinstance(key, StringTypes):
                return False
        return True

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
                    return False
        elif isinstance(partitions, StringTypes):
            return True
        elif isinstance(partitions, dict):
            for platforms, clause in partitions.items():
                if platforms and not isinstance(frozenset):
                    return False
                if clause and not isinstance(clause, Predicate):
                    return False
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
        if not Table.check_fields(fields):
            raise TypeError("Table: __init__: invalid parameter fields '%s' (type: %r)" % (fields, type(fields)))
        if not Table.check_keys(keys):
            raise TypeError("Table: __init__: invalid parameter keys: %s (type: %r)" % (keys, type(keys)))
        if not Table.check_partitions(partitions):
            raise TypeError("Table: __init__: invalid parameter partitions: %s (type: %r)" % (partitions, type(partitions)))

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
                field_name = field.field_name
                self.fields[field_name] = field
        elif isinstance(fields, dict):
            self.fields = fields

        self.keys = to_frozenset(keys)
        self.cost = cost
        # TODO There will also be a list that the platform cannot provide, cf sources[i].fields

    #-----------------------------------------------------------------------
    # Outputs 
    #-----------------------------------------------------------------------

    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "<{%s}::%s fields = {%s} keys = {%s}>" % (
            ', '.join([p            for p in sorted(self.get_platforms())]),
            self.name,
            ', '.join([f.field_name for f in sorted(self.get_fields())]),
            self.keys
        )

    def __repr__(self):
        """
        \brief Convert a Table instance into a string ('%r')
        \return The corresponding string
        """
        platforms = self.get_platforms()
        if platforms:
            return "<{%s}::%s>" % (', '.join([p for p in sorted(platforms)]), self.name)
        else:
            return self.name

    #-----------------------------------------------------------------------
    # Accessors 
    #-----------------------------------------------------------------------

    @returns(bool)
    def is_key(self, key):
        """
        \brief Test whether a field is a key of this table
        \param key The name of the field.
            You might pass an tuple or a list of fields (string or MetadataField)
            if your testing a composite key.
        \return True iif only this is a key
        """
        if isinstance(key, (list, set, frozenset)):
            key = tuple(key)
        elif isinstance(key, (StringTypes, MetadataField)):
            key = (key,)
        elif not isinstance(key, tuple):
            raise TypeError("is_key: %s must be a list, a tuple, a set, a frozenset or a string" % key)
        key = tuple([k if isinstance(k, StringTypes) else k.field_name for k in key])
        return key in self.keys

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

    @returns(bool)
    def erase_key(self, key_to_remove):
        """
        \brief Remove a key from the table
        \param key_to_remove A set of field names describing the key 
        \return True iif the key has been successfully removed
        """
        if not isinstance(key_to_remove, set):
            raise TypeError("Invalid type: %r is of type %r (must inherits set)" % (key_to_remove, type(key_to_remove)))

        for key in self.keys: 
            if key_to_remove == set(key):
                keys = set(self.keys)
                keys.erase(key)
                self.keys = frozenset(keys)
                return True
        return False

    def get_field(self, field_name):
        """
        \brief Retrieve a MetadataField instance stored in self according to
            its field name.
        \param field_name The name of the requested field.
        \return The MetadataField instance identified by 'field_name'
        """
        # Retrieve the field name
        if isinstance(field_name, MetadataField):
            field_name = field_name.field_name
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

    @returns(set)
    def get_fields_from_keys(self):
        """
        \return A set of tuple of MetadataField.
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a MetadataField.
        """
        fields_keys = set() 
        for key in self.keys:
            if isinstance(key, (tuple, list)):
                cur_key = []
                for field in key:
                    if isinstance(field, StringTypes):
                        cur_key.append(self.get_field(field))
                    elif isinstance(field, MetadataField):
                        cur_key.append(field)
                    else:
                        raise TypeError("Invalid field: %r (type not supported: %r)" % (field, type(field)))
                fields_keys.add(tuple(cur_key))
            elif isinstance(key, StringTypes):
                fields_keys.add(self.get_field(key))
            elif isinstance(key, MetadataField):
                fields_keys.add(key)
            else:
                raise TypeError("Invalid key: %r (type not supported: %r)" % (key, type(key)))
        return fields_keys

    @returns(set)
    def get_names_from_keys(self):
        """
        \return A set of tuple of field names
            Each tuple corresponds to a key of 'self'.
            Each element of these tuples is a String.
        """
        names_keys = set() 
        for key in self.keys:
            if isinstance(key, (tuple, list)):
                name_key = []
                for field in key:
                    if isinstance(field, StringTypes):
                        name_key.append(field)
                    elif isinstance(field, MetadataField):
                        name_key.append(field.field_name)
                    else:
                        raise TypeError("Invalid field: %r (type not supported: %r)" % (field, type(field)))
                names_keys.add(tuple(name_key))
            elif isinstance(key, StringTypes):
                names_keys.add(key)
            elif isinstance(key, MetadataField):
                names_keys.add(key.field_name)
            else:
                raise TypeError("Invalid key: %r (type not supported: %r)" % (key, type(key)))
        return names_keys

    @returns(set)
    def get_types_from_keys(self):
        """
        \return A set of tuple of types 
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a typename 
        """
        fielded_keys = self.get_fields_from_keys()
        ret = set()
        for fielded_key in fielded_keys:
            if len(list(fielded_key)) > 1:
                type_key = []
                for fielded_key_elt in fielded_key:
                    type_key.append(fielded_key_elt.type)
                ret.add(tuple(type_key))

            fielded_key = list(fielded_key)[0]
            if isinstance(fielded_key, MetadataField):
                ret.add(fielded_key.type)
            else:
                raise TypeError("Invalid field: %r (type not supported: %r)" % (field, type(field)))
        return ret

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
    #-----------------------------------------------------------------------

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
            if field.field_name == table.name or field.type == table.name:
                connecting_fields.add(field)
        return connecting_fields 

    @returns(set)
    def get_provider_fields(self, connecting_fields):
        """
        \brief (Internal use, since this function is called in a specific context)
            Find among connecting fields those verifying (allow to deduce whether self ~~> table)
            f not in u.k (P2)
        \param connecting_fields Connecting fields (set of MetadataField)
            \sa get_connecting_fields()
        \return The set of MetadataField verifying (P2)
        """
        provider_fields = set()
        for field in connecting_fields: 
            key_types = self.get_types_from_keys()
            for key_type in key_types:
                if isinstance(key_type, (tuple, list, set, frozenset)):
                    # Skip composite keys
                    continue
                elif isinstance(key_type, StringTypes):
                    if field.type == key_type:
                        provider_fields.add(field) 
                else:
                    raise TypeError("Invalid key type %r, type name expected (e.g. string)" % key_type)
        return provider_fields 

    @returns(bool)
    def includes(self, table):
        """
        \brief (Internal use, since this function is called in a specific context)
            Test whether self ==> table
            Example: tophat::ip ==> {tophat, sonoma}::ip
        \param table The target candidate table
        \return True iif u ==> v
        """
        return self.name == table.name 

    @returns(bool)
    def inherits(self, table):
        """
        \brief (Internal use, since this function is called in a specific context)
            Test whether self inherits table
        \param table The target candidate table
        \return True iif u --> v
        """
        return (table.name,) in self.get_names_from_keys()

    def get_relation(self, table):
        """
        \brief Compute which kind of relation connects
            the "self" Table (source node) to the "table"
            Table (target node).
        \param table The target table
        \return
            - None if the both tables are unrelated
            - Otherwise, a tuple made of
                - a string: "==>", "-->", "~~>"
                - a set of MetadataField that will be stored in the arc 
        """
        connecting_fields = self.get_connecting_fields(table)
        if connecting_fields != set():
            provider_fields = self.get_provider_fields(connecting_fields)
            if provider_fields != set():
                if self.includes(table):
                    return ("==>", None)
                elif self.inherits(table):
                    return ("-->", connecting_fields) 
                else:
                    return ("~~>", provider_fields)
        return None

#
#    @returns(bool)
#    def determines(self, table):
#        """
#        \brief Test whether "self" determines "table" table.
#            u --> v iif
#                exists k | u.k == v (foreign key)
#                u.p == v.p          (platform equality)
#        Example: tophat::agent --> tophat::ip
#        \sa tophat/util/dbgraph.py
#        \param table The target candidate table
#        \return True iif self --> table
#        """
#        if set(self.get_platforms()) == set(table.get_platforms()):
#            keys = self.get_fields_from_keys()
#            for key in keys:
#                if len(list(key)) > 1:
#                    #print "determines: ignoring composite key"
#                    continue
#                else:
#                    key = list(key)[0]
#               # print "> > key.type = %r ?= table.name = %r" % (key.type, table.name)
#                if key.type == table.name or key.field_name == table.name:
#                    return True
#        return False
#
#    @returns(tuple)
#    def get_determinant(self, table):
#        if self.get_platforms() == table.get_platforms():
#            keys = self.get_fields_from_keys()
#            for key in keys:
#                if len(list(key)) > 1:
#                    continue
#                else:
#                    key = list(key)[0]
#                if key.type == table.name or key.field_name == table.name:
#                    return (set([key.type]), None)
#        raise ValueError("%r does not determine %r" % (self, table))
#
#    @returns(bool)
#    def includes(self, table):
#        """
#        \brief Test whether "self" includes "table" table.
#            u ==> v iif
#                u.p <= v.p (platform inclusion)
#                u.n == v.n (name equality)
#                u.f <= v.f (field inclusion)
#        Example: tophat::ip ==> {sonoma,tophat}::ip
#        \sa tophat/util/dbgraph.py
#        \param table The target candidate table
#        \return True iif self ==> table
#        """
#        if self.get_platforms() <= table.get_platforms() and table.name == self.name: 
#            fields_self  = set([(field.field_name, field.type) for field in self.get_fields()])
#            fields_table = set([(field.field_name, field.type) for field in table.get_fields()])
#            return fields_table <= fields_self
#        return False 
#
#    @returns(bool)
#    def provides(self, table):
#        """
#        \brief Test whether "self" provides a "table" table.
#            u ~~> f iif:
#            #    \exists k | v.k \in u.f (foreign key)
#            #    u.p == v.p (Jordan: only if the key is LOCAL, not implemented)
#                \exists f | u.f.type == v or u.f.name == v 
#            Example:
#                tophat::traceroute ~~> tophat::agent
#        \sa tophat/util/dbgraph.py
#        \param table The target candidate table
#        \return True iif self ~~> table
#        """
##        if self.get_platforms() == table.get_platforms():
##            for field in self.get_fields():
##                if field.type == table.name or field.field_name == table.name:
##                    return True
#
#        
#        # JORDAN: commented platform check since this only applies to LOCAL keys
#        #if self.get_platforms() == table.get_platforms():
#        for key in table.keys:
#            # We ignore composite key (e.g. (source, destination, ts))
#            if isinstance(key, (list, tuple, set, frozenset)):
#                if len(key) > 1:
#                    print "provides(): W: Ignoring key %r" % key
#                    continue
#                key = list(key)[0]
#
#            if isinstance(key, MetadataField):
#                key_type = key.type
#                key_name = key.field_name
#            elif isinstance(key, StringTypes):
#                key_type = table.get_field(key).type
#                key_name = key
#
#            if table.get_field(key_name).type == key_type:
#                # Jordan: added this test so that hop.ip does not point to
#                # agent
#                if table.name == key_name:
#                    return True
#        return False
#
#    @returns(tuple)
#    def get_provider(self, table):
##        if self.get_platforms() == table.get_platforms():
##            for field in self.get_fields():
##                if field.type == table.name or field.field_name == table.name:
##                    return (set([field.field_name]), None)
#
#        for key in table.keys:
#            # We ignore composite key (e.g. (source, destination, ts))
#            if isinstance(key, (list, tuple, set, frozenset)):
#                if len(key) > 1: continue
#                key = list(key)[0]
#
#            if isinstance(key, MetadataField):
#                key_type = key.type
#                key_name = key.field_name
#            elif isinstance(key, StringTypes):
#                key_type = table.get_field(key).type
#                key_name = key
#
#            if table.get_field(key_name).type == key_type:
#                if table.name == key_name:
#                    return (set([table.name]), None)
#
#        raise ValueError("%r does not provide %r" % (self, table))
#
#
