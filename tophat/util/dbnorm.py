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
from tophat.core.field             import Field
from tophat.core.method            import Method 
from types                         import StringTypes
from tophat.util.type              import returns, accepts
from copy                          import copy, deepcopy

#------------------------------------------------------------------------------

class Determinant:
    """
    A Determinant models the left operand of a "rule" that allow to retrieve one or more field

    Example: 
        Consider a table/method named t provided by a platform P providing fields y, z for a key k:

            P::t{k*,y, z}

        Then (t, k) is a Determinant of fields {y, z}
        The corresponding functionnal dependancy is:

            (t, k) --> {y,z}

        t is required in the determinant since it leads to different semantic meanings.
        For example if t' inherits t and uses the same key(s), t' only provide information for a subset of keys of t.
    """

    @staticmethod
    def check_init(key, method_name):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        if not isinstance(key, Key):
            raise TypeError("Invalid key %r (type = %r)" % (key, type(key)))
        if not isinstance(method_name, StringTypes):
            raise TypeError("Invalid method_name %r (type = %r)" % (method_name, type(method_name))) 

    def __init__(self, key, method_name):
        """
        \brief Constructor of Determinant
        \brief key The key of the determinant (k)
        \param method_name The name of the table/method_name (m)
        """
        Determinant.check_init(key, method_name)
        self.key = key
        self.method_name = method_name

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
    def get_method_name(self):
        """
        \returns A string (the method_name related to this determinant) (m)
        """
        return self.method_name

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
        return self.get_key() == x.get_key() and self.get_method_name() == x.get_method_name()

    @returns(str)
    def __str__(self):
        """
        \return The (verbose) string representing a Determinant instance
        """
        return self.__repr__()

    @returns(str)
    def __repr__(self):
        """
        \return The (synthetic) string representing a Determinant instance
        """
        return "(%s, %s)" % (
            self.get_method_name(),
            self.get_key()
        )

#------------------------------------------------------------------------------

class Fd:
    """
    Functionnal dependancy.
    determinant --> fields
    For each field, we store which Method(s) (e.g. (platform, table_name))
    can provide this field.
    """
    @staticmethod
    def check_init(determinant, map_field_methods):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        if not isinstance(determinant, Determinant):
            raise TypeError("Invalid determinant %s (%s)" % (determinant, type(determinant)))
        if not isinstance(map_field_methods, dict):
            raise TypeError("Invalid map_field_methods %s (%s)" % (map_field_methods, type(map_field_methods)))
        for field, methods in map_field_methods.items():
            if not isinstance(field, Field):
                raise TypeError("Invalid field %s (%s)" % (field, type(field)))
            if not isinstance(methods, set):
                raise TypeError("Invalid methods %s (%s)" % (methods, type(methods)))
            for method in methods:
                if not isinstance(method, Method):
                    raise TypeError("Invalid method %s (%s)" % (method, type(method)))

    def __init__(self, determinant, map_field_methods):
        """
        \brief Constructor of Fd 
        \param determinant A Determinant instance (left operand of the fd)
        \param map_field_methods A dictionnary storing for each field the corresponding methods
            that we can use to retrieve it.
        """
        Fd.check_init(determinant, map_field_methods) 
        self.determinant = determinant
        self.map_field_methods = map_field_methods 

    def set_key(self, key):
        self.determinant.set_key(key)

    @returns(set)
    def get_platforms(self):
        """
        \returns A set of strings (the platforms able to provide 
            every fields involved in the key of this fd)
        """
        platforms = set()
        first = True
        print "get_platforms %s" % self
        for field in self.get_determinant().get_key():
            platforms_cur = set([method.get_platform() for method in self.map_field_methods[field]])
            if first:
                platforms |= platforms_cur 
            else:
                platforms &= platforms_cur 
        return platforms

    def get_determinant(self):
        return self.determinant

    @returns(set)
    def get_fields(self):
        return set(self.map_field_methods.keys())

    def get_field(self):
        if len(self.map_field_methods) != 1:
            raise ValueError("This fd has not exactly one field: %r" % self)
        for field in self.map_field_methods.keys():
            return field

    @returns(set)
    def split(self):
        """
        \brief Split a fd
            Example: [k -> {f1, f2...}] is split into {[k -> f1], [k -> f2], ...}
        \returns A set of Fd instances
        """
        fds = set()
        determinant = self.get_determinant()
        for field, methods in self.map_field_methods.items():
            for method in methods:
                map_field_methods = dict()
                map_field_methods[field] = set()
                map_field_methods[field].add(method)
                fds.add(Fd(determinant, map_field_methods))
        return fds 

    #@returns(str)
    def __str__(self):
        cr = ''
        cr2 = ''
        if len(self.get_fields()) > 1 :
            cr ='\n'
            cr2 ='\n\t'
        
        return "[%s => {%s%s%s}]" % (
            self.get_determinant(),
            cr2,
            cr2.join([
                "%r\t(via {%s})" % (
                    field,
                    ' '.join(['%r' % method for method in methods])
                ) for field, methods in self.map_field_methods.items()
            ]),
            cr
        )

    @returns(str)
    def __repr__(self):
        return "[%r => {%s}]" % (
            self.get_determinant(),
            ' ,'.join(["%r" % field for field in self.get_fields()])
        )

    @returns(dict)
    def get_map_field_methods(self):
        return self.map_field_methods

    def __ior__(self, fd):
        """
        \brief |= overloading
        \param fd A Fd instance that we merge with self
        \return self
        """
        if (self.get_determinant() == fd.get_determinant()) == False:
            raise ValueError("Cannot call |= with parameters self = %r and fd = %r" % (self, fd))
        for field, methods in fd.map_field_methods.items():
            if field not in self.map_field_methods.keys():
                self.map_field_methods[field] = set()
            self.map_field_methods[field] |= methods
        return self

    def merge_methods(self, fd):
        """
        \brief (Internal usage)
        \param fd The Fd instance that we "merge" with self. self and fd must have
            the same output fields but may have different keys.
        """
        if self.get_fields() != fd.get_fields():
            raise ValueError("Cannot call merge_methods with self = %r and fd = %r" % (self, fd))
        for field in self.map_field_methods.keys():
            self.map_field_methods[field] |= fd.map_field_methods[field]

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
        \brief Constructor of Fds
        \param fds A set of Fd instances
        """
        Fds.check_init(fds)
        # /!\ Don't call set.__init(fds), copy explicitly each fd 
        for fd in fds:
            self.add(fd)

    #@returns(Fds)
    def collapse(self):
        """
        \brief Aggregate each Fd of this Fds by Key 
        \returns The corresponding Fds instance 
        """
        map_key_fd = {}
        for fd in self:
            key = fd.get_determinant().get_key()
            if key not in map_key_fd.keys():
                map_key_fd[key] = fd
            else:
                map_key_fd[key] |= fd
        return Fds(set(map_key_fd.values()))

    #@returns(Fds)
    def split(self):
        """
        \brief Split a Fds instance
        \returns The corresponding Fds instance 
        """
        fds = Fds()
        for fd in self:
            fds |= fd.split()
        return fds 

    @returns(str)
    def __str__(self):
        return '\n'.join(["%s" % fd for fd in self])

    @returns(str)
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
            if not isinstance(field, Field):
                raise TypeError("Invalid attribute: type (%r)" % type(field))

    @staticmethod
    @accepts(set, Fds)
    @returns(set)
    def closure(x, fds):
        """
        \brief Compute the closure of a set of attributes under the
            set of functional dependencies
            \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p7
        \param x A set of Field instances 
        \param fds A Fds instance
        \return A set of Field instances 
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
            name = table.get_name()
            for key in table.get_keys():
                for field in table.get_fields():
                    map_field_methods = dict()
                    methods = set()
                    for platform in table.get_platforms():
                        methods.add(Method(platform, name))
                    map_field_methods[field] = methods
                    fds.add(Fd(Determinant(key, name), map_field_methods))
        return fds

    @staticmethod
    @accepts(Fds)
    @returns(tuple)
    def fd_minimal_cover(fds):
        """
        \brief Compute the functionnal dependancy minimal cover
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p11
        \param fds A Fds instance 
        \return A couple made of
            - a Fds instance (fd kept, e.g. min cover)
            - a Fds instance (fd removed)
        """
        g = fds.split()                                     # replace {[x -> {a1, ..., an}]} by g = {[x -> a1], ..., [x -> an]}

        fds_removed = Fds()
        g_copy = g.copy()
        for fd in g_copy:                                   # for each fd = [x -> a]:
            g2 = Fds(set([f for f in g if fd != f]))        #   g' = g \ {fd}
            x  = fd.get_determinant().get_key()
            a  = fd.get_field()
            x_plus = DBNorm.closure(set(x), g2)             #   compute x+ according to g'
            if a in x_plus:                                 #   if a \in x+:
                fds_removed.add(fd)
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

        return (g, fds_removed) 

    @staticmethod
    @returns(Fds)
    def reinject_fds(fds, fds_removed):
        for fd in fds_removed:
            key = fd.get_determinant().get_key()
            if not key.is_composite() and key.get_field() == fd.get_field():
                # The min cover algorithm has removed x --> x dependancies
                # However we need these fds to deduce the entry points of
                # the table and how to retrieve this field. 
                # fields |= key
                fds.add(fd)
            else:
                # During the min cover algorithm, this fd has been removed
                # However its method remains relevant, so reinject this
                # method in the fd set.
                merged = False
                fields = fd.get_fields()
                for f in fds:
                    if f.get_fields() == fields:
                        # update fd
                        fds.remove(f)
                        f.merge_methods(fd)
                        fds.add(f)
                        merged = True
                if not merged:
                    raise Exception("Can't merge %s with %s" % (fd, fds))
        return fds

    @returns(list)
    def to_3nf(self):
        """
        \brief Compute a 3nf schema according to self.tables
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
        \return The corresponding list of Table instances in the 3nf schema
        """
        # Compute functional dependancies
        print "-" * 100
        print "Compute functional dependancies"
        print "-" * 100
        fds = self.make_fd_set()
        print "%s" % fds

        # Compute the map which refer for each key the platforms
        # which support this key 
        map_key_platforms = dict()
        for fd in fds:
            key = fd.get_determinant().get_key()
            if key not in map_key_platforms.keys():
                map_key_platforms[key] = set()
            for table in self.tables:
                if table.has_key(key):
                    map_key_platforms[key] |= table.get_platforms()

        # Find a minimal cover
        print "-" * 100
        print "Minimal cover"
        print "-" * 100
        (fds_min_cover, fds_removed) = DBNorm.fd_minimal_cover(fds)
        print "%s" % fds_min_cover

        print "-" * 100
        print "Reinject removed Fds"
        print "-" * 100
        fds = DBNorm.reinject_fds(fds_min_cover, fds_removed)
        print "%s" % fds

        print "-" * 100
        print "Aggregate Fds by Key"
        print "-" * 100
        fds = fds.collapse()
        print "%s" % fds

        # ... create relation R = (X, A1, A2, ..., An)
        relations = []
        for fd in fds:
            # Search source tables related to the corresponding key and values
            key = fd.get_determinant().get_key()
            sources = [table for table in self.tables if table.has_key(key)]
            if not sources:
                raise Exception("No source table found with key %s" % key)

            fields = fd.get_fields()
            relations.append(Table(
                fd.get_platforms(),
                fd.get_map_field_methods(),
                key.get_name(),
                fields,
                [key]
            ))
        return relations

