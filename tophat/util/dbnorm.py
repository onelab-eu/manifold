#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# DBNorm 
# Compute a 3nf schema
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from tophat.core.table             import Table
from tophat.core.key               import Key 
from tophat.metadata.MetadataField import MetadataField
from types                         import StringTypes
from tophat.util.type              import returns, accepts
from copy                          import copy, deepcopy

#------------------------------------------------------------------------------

class Determinant:
    """
    A Determinant models the left operand of a "rule" that allow to retrieve one or more field

        Notations:
            {platforms}::method{key*, fields}

        Example: 
            Consider a method m provided by a platform P providing fields y, z for a key k:
            P::m{k*,y, z}
            Then (m, k) is a Determinant of fields {y, z}
            and (m, k) --> {y,z} is a functional dependancy (fd) provided by P::m

    To compute the 3nf schema, ONLY the method and the key (m, k) are relevant
    and that is why Determinant only stores the method and the key, not the platform.
    Indeed if we consider a Determinant to be (P, m, k) rather than (m, k), we
    could miss some functional dependancies the right 3nf schema (see example above).

        Example :
            Inputs:

                T::x{x*, y, z} T::z{z*, t}
                S::x{x*, y, z, t}

            If we consider platform in the determinant, it leads to:

                3nf table            |  Method
                ---------------------+--------------------
                {S, T}x::{x*, y, z}  |  (S::x and T::x)
                T::z{z*, t}          |  (T::z)
                (so we don't detect that S::z allows to deduce {S,T}::t)

            ... instead of:

                3nf table            |  Method
                ---------------------+--------------------
                {S, T}::x{x*, y, z}  |  (S::x and T::x)
                {S, T}::z{z*, t}     [  (S::x and T::z)
    """

    @staticmethod
    def check_init(key, method):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        if not isinstance(key, Key):
            raise TypeError("Invalid key %r (type = %r)" % (key, type(key)))
        if not isinstance(method, StringTypes):
            raise TypeError("Invalid method %r (type = %r)" % (method, type(method))) 

    def __init__(self, key, method):
        """
        \brief Constructor
        \brief key The key of the determinant (k)
        \param method The name of the table/method (m)
        """
        Determinant.check_init(key, method)
        self.key = key
        self.method = method

    def set_key(self, key):
        if not isinstance(key, Key):
            raise TypeError("Invalid key %s (%s)" % (key, type(key)))
        self.key = key

    @returns(Key)
    def get_key(self):
        """
        \return The Key instance related to this determinant (k)
        """
        return self.key

    @returns(str)
    def get_method(self):
        """
        \returns A string (the method related to this determinant) (m)
        """
        return self.method

    def __hash__(self):
        """
        \returns The hash of a determinant 
        """
        return hash(self.get_key())

    @returns(bool)
    def __eq__(self, x):
        """
        \brief Compare two Determinant instances
        \param x The Determinant instance compared to "self"
        \return True iif self == x
        """
        if not isinstance(x, Determinant):
            raise TypeError("Invalid paramater %r of type %r" % (x, type(x)))
        return self.get_key() == x.get_key() and self.get_method() == x.get_method()

    @returns(unicode)
    def __str__(self):
        """
        \return The (verbose) string representing a Determinant instance
        """
        return (
            self.get_key(),
            self.get_method()
        ).__str__()

    @returns(str)
    def __repr__(self):
        """
        \return The (synthetic) string representing a Determinant instance
        """
        return "(%r, METH(%s))" % (
            self.get_key(),
            self.get_method()
        )

#------------------------------------------------------------------------------

class Fd:
    """
    Functionnal dependancy.
    This represents what MetadataField we can retrieve for a given Determinant.
    """
    @staticmethod
    def check_init(determinant, fields, map_platform_method):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        if not isinstance(map_platform_method, dict):
            raise TypeError("Invalid map_platform_method %s (%s)" % (map_platform_method, type(map_platform_method)))
        for platform, method in map_platform_method.items():
            if not isinstance(platform, StringTypes):
                raise TypeError("Invalid platform %s (%s)" % (platform, type(platform)))
            if not isinstance(method, StringTypes):
                raise TypeError("Invalid method %s (%s)" % (method, type(method)))
        if not isinstance(determinant, Determinant):
            raise TypeError("Invalid determinant %s (%s)" % (determinant, type(determinant)))
        if not isinstance(fields, (list, set, frozenset)):
            raise TypeError("Invalid fields %s (%s)" % (fields, type(fields)))
        for field in fields:
            if not isinstance(field, MetadataField):
                raise TypeError("Invalid field %s (%s) in fields %s" % (field, type(fields), MetadataField))

    def __init__(self, determinant, fields, map_platform_method):
        """
        \brief Constructor 
        \param determinant A Determinant instance
        \param fields A frozenset of MetadataField instances
        \param map_platform_method A dictionnary which associates a platform name (String) to a method (String)
        """
        Fd.check_init(determinant, fields, map_platform_method)
        self.determinant = determinant
        self.fields = set(fields)
        self.map_platform_method = map_platform_method

    def get_map_platform_method(self):
        return self.map_platform_method

    def set_key(self, key):
        self.determinant.set_key(key)

    @returns(frozenset)
    def get_platforms(self):
        """
        \returns A frozenset of string (the platforms related to this Fd) (P)
        """
        return frozenset(self.map_platform_method.keys())

    #@returns(Determinant)
    def get_determinant(self):
        return self.determinant

    def get_field(self):
        if len(self.fields) != 1:
            raise ValueError("This fd has not exactly one field: %r" % self)
        return list(self.fields)[0]

    @returns(set)
    def get_fields(self):
        return self.fields

    @returns(set)
    def split(self):
        """
        \brief Split a fd
            Example: [k -> {f1, f2...}] is split into {[k -> f1], [k -> f2], ...}
        \returns A set of Fd instances
        """
        fds = set()
        for field in self.get_fields():
            fields = set()
            fields.add(field)
            fds.add(Fd(
                self.get_determinant(),
                fields,
                self.get_map_platform_method()
            ))
        return fds 

    @returns(unicode)
    def __str__(self):
        return "%10s [%s => %s]" % (
            self.map_platform_method,
            self.get_determinant(),
            self.get_fields(),
        )

    @returns(unicode)
    def __repr__(self):
        return "{%10s} [%r => %r]" % (
            ', '.join(["%s::%s" % (p,m) for p, m in self.map_platform_method.items()]),
            self.get_determinant(),
            self.get_fields()
        )

    def __ior__(self, x):
        """
        \brief |= overloading
        \param x A Fd instance that we merge with self
        \return self
        """
        if self.get_determinant() != x.get_determinant():
            raise ValueError("Cannot call |= with parameter self = %r and x = %r" % (self, x))
        self.fields |= x.get_fields()
        for p, m in x.get_map_platform_method.items():
            self.map_platform_method[p] = m

#------------------------------------------------------------------------------

class Fds(set):
    """
    Functionnal dependancies.
    """
    @staticmethod
    def check_init(fds):
        """
        \brief (Internal use)
        \param fds A set of Fd instances
        """
        if not isinstance(fds, set):
            raise TypeError("Invalid fds %s (type %s)" % (fds, type(fds)))
        for fd in fds:
            if not isinstance(fd, Fd):
                raise TypeError("Invalid fd %s (type %s)" % (fd, type(fd)))

    def __init__(self, fds = set()):
        """
        \brief Constructor
        \param fds A set of Fd instances
        """
        Fds.check_init(fds)
        print "__init__: %r" % fds
        set.__init__(fds)

    def collapse(self):
        """
        \brief Aggregate each fd by determinant 
        """
        map_determinant_fd = {}
        for fd in self:
            determinant = fd.get_determinant()
            if determinant not in map_determinant_fd.keys():
                map_determinant_fd[determinant] = fd
            else:
                map_determinant_fd[determinant] |= fd
        return Fds(set(map_determinant_fd.values()))

    #@returns(Fds)
    def split(self):
        """
        \brief Split a Fds instance
        \returns A set of Fd instances
        """
        fds = Fds()
        for fd in self:
            fds |= fd.split()
        return fds 

    def __str__(self):
        return '\n'.join(["%s" % fd for fd in self])

    def __repr__(self):
        return '\n'.join(["%r" % fd for fd in self])

#------------------------------------------------------------------------------

class DBNorm:
    """
    Database schema normalization support
    http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf
    """

    def __init__(self, tables = None):
        """
        \brief Initializes the set of tables
        \param tables The set of tables (Table instances)
        \sa tophat/core/table.py
        """
        if tables == None:
            self.tables = []
            self.tables_3nf = None
        else:
            self.tables = tables
            self.tables_3nf = self.to_3nf()

    @staticmethod
    def check_closure(fields, fds):
        """
        \brief (Internal use)
            Check wether paramaters passed to closure()
            are well-formed.
        """
        if not isinstance(fds, Fds):
            raise TypeError("Invalid type of fields (%r)" % type(fds))
        if not isinstance(fields, (frozenset, set)):
            raise TypeError("Invalid type of fields (%r)" % type(fields))
        for field in fields:
            if not isinstance(field, MetadataField):
                raise TypeError("Invalid attribute: type (%r)" % type(field))

    @staticmethod
    @accepts(set, Fds)
    @returns(set)
    def closure(x, fds):
        """
        \brief Compute the closure of a set of attributes under the
            set of functional dependencies
            \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p7
        \param x A set of MetadataField instances 
        \param fds A Fds instance
        \return A set of MetadataField instances 
        """
        DBNorm.check_closure(x, fds)
        x_plus = set(x)                              # x+ = x
        while True:                                  # repeat
            old_x_plus = x_plus.copy()               #   temp_x+ = x+
            for fd in fds:                           #   for each fd (y -> z)
                key = fd.get_determinant().get_key() #     get y
                if key <= x_plus:                    #     if y \subseteq x+
                    x_plus |= fd.get_fields()        #       x+ = x+ \cup z
            if old_x_plus == x_plus:                 # until temp_x+ = x+
                break
        return x_plus
    
    @returns(Fds)
    def make_fd_set(self):
        """
        \brief Compute the set of functionnal dependancies
        \returns A Fds instance
        """
        fds = Fds() 
        for table in self.tables:
            for key in table.get_fields_from_keys():
                map_platforms_method = {}
                method = table.get_name()
                for platform in table.get_platforms():
                    map_platforms_method[platform] = method
                fds.add(
                    Fd(
                        Determinant(key, method),
                        table.get_fields(),
                        map_platforms_method
                    )
                )
        #return fds.collapse()
        return fds

    @staticmethod
    @accepts(Fds)
    @returns(Fds)
    def fd_minimal_cover(fds):
        """
        \brief Compute the functionnal dependancy minimal cover
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p11
        \param fds A Fds instance 
        \return The corresponding min cover (Fds instance)
        """
        g = fds.split()                                     # replace {[x -> {a1, ..., an}]} by g = {[x -> a1], ..., [x -> an]}
        print ">>>>>>>>>"
        print "fds =\n%r" % fds
        print "g(%r) =\n%r" % (len(g),g)

        g_copy = g.copy()
        for fd in g_copy:                                   # for each fd = [x -> a]:
            print "---"
            #print "fd = %r" % fd
            g2 = Fds(set([f for f in g if fd != f]))        #   g' = g \ {fd}

            #####
            print "%r" % g
            for f in g:
                print "%r != %r ? %r" % (fd, f, fd != f)
            print "==> g2 = %r" % [f for f in g if fd != f]
            print "==> g2 = %r" % set([f for f in g if fd != f])
            print "==> g2 = %r" % g2 
            #print "g(%r) =\n%r" % (len(g),g)
            #print "g2(%r) =\n%r" % (len(g2),g2)
            #print "g_copy(%r) =\n%r" % (len(g_copy),g_copy)
            x  = fd.get_determinant().get_key()
            a  = fd.get_field()
            x_plus = DBNorm.closure(set(x), g2)             #   compute x+ according to g'
            if a in x_plus:                                 #   if a \in x+:
                g = g2                                      #     g = g'

        for fd in g.copy():                                 # for each fd = [x -> a] in g:
            x = fd.get_determinant().get_key()
            if x.is_composite():                            #   if x has multiple attributes:
                for b in x:                                 #     for each b in x:

                    x_b = Key([xi for xi in x if xi != b])  #       x_b = x - b
                    g2  = Fds([f for f in g if fd != f])    #       g'  = g \ {fd} \cup {fd'}
                    fd2 = fd.copy()                         #          with fd' = [(x - b) -> a]
                    fd2.set_key(x_b)
                    g2.add(fd2)
                    x_b_plus = DBNorm.closure(set(x_b), g2) #       compute (x - b)+ with repect to g'

                    if b in x_b_plus:                       #       if b \subseteq (x - b)+:
                        g = g2                              #         replace [x -> a] in g by [(x - b) -> a]

        return g

    @returns(list)
    def to_3nf(self):
        """
        \brief Compute a 3nf schema according to self.tables
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
        \return The corresponding list of Table instances in the 3nf schema
        """
        # Compute functional dependancies
        fd_set = self.make_fd_set()
        print "\nmake_fd_set:\n%r" % fd_set

        # Find a minimal cover
        fd_set = DBNorm.fd_minimal_cover(fd_set)
        print "\nfd_minimal_cover:\n%r" % fd_set

        # Aggregate functionnal dependancies by determinant
        fds = fd_set.collapse()
        print "\ncollapse:\n%r" % fd_set
        
        # ... create relation R = (X, A1, A2, ..., An)
        relations = []
        for fd in fds:
            # Search source tables related to the corresponding key key and values
            key = fd.get_determinant().get_key()
            platforms = fd.get_platforms()
            method = determinant.get_method()
            sources = [table for table in self.tables if table.has_key(key)]
            if not sources:
                raise Exception("No source table found with key %s" % key)

            #DEBUG
            print "a) fields = ", fields
            fields = list(fields)
            for key_elt in key:
                if not isinstance(key_elt, MetadataField):
                    raise TypeError("Inconsistent key %r, %r is not of type MetadataField" % (key, key_elt))
                fields.append(key_elt)
            print "b) fields = ", fields
            t = Table(platforms, None, method, fields, [key]) # None = methods
            relations.append(t)
        return relations

