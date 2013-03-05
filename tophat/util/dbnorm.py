#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# DBNorm 
# Compute a 3nf schema
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr>

# Hypotheses:
# - unique naming: columns having the same name in several table have the same semantic
#   - example: T1.date and T2.date and T1 -> T2, "date" refers to the same date 
#   - if not, the both date should be explictely disambiguited (for instance T1_date and T2_date)
#      - example: traceroute.date (date of the traceroute) and traceroute.agent.date (date of the last boot)
#        is unallowed since "date" does not have the same meaning in the traceroute and the agent tables.
# - underlying ontology: an given column has always the name it would have in the underlying ontology
#   - example: P1::traceroute.destination and P2::traceroute.target is unallowed since
#     destination and target have the same meaning.
#

import copy
from types                         import StringTypes

from tophat.core.table             import Table
from tophat.core.key               import Key 
from tophat.core.field             import Field
from tophat.core.method            import Method 
from tophat.util.type              import returns, accepts
from tophat.util.dbgraph           import DBGraph

#------------------------------------------------------------------------------

class Determinant(object):
    """
    A Determinant models the left operand of a "rule" that allow to retrieve
    one or more field thanks to a key.

    Example: 

        Consider a table/method named t provided by a platform P providing
        fields y, z for a key k:

            P::t{k*,y, z}

        Then (t, k) is a Determinant of fields {y, z}
        The corresponding functionnal dependancy is:

            (t, k) --> {y,z}

        t is required in the Determinant since it leads to different
        semantic meanings. For example if t' inherits t and uses the same
        key(s), t' only provide information for a subset of keys of t.
    """

    @staticmethod
    def check_init(key, method_name):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        assert isinstance(key, Key),                 "Invalid key %r (type = %r)" % (key, type(key))
        assert isinstance(method_name, StringTypes), "Invalid method_name %r (type = %r)" % (method_name, type(method_name)) 

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
        """
        \brief Set the key related to this Determinant
        \param key A Key instance
        """
        assert isinstance(key, Key), "Invalid key %s (%s)" % (key, type(key))
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
        assert isinstance(x, Determinant), "Invalid paramater %r (type %r)" % (x, type(x))
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

class Fd(object):
    """
    Functionnal dependancy.
    Determinant --> {Field}
    For each field, we store which Method(s) (e.g. (platform, table_name))
    can provide this field.
    """
    @staticmethod
    def check_init(determinant, map_field_methods):
        """
        \brief (Internal use)
            Check whether parameters passed to __init__ are well-formed 
        """
        assert isinstance(determinant, Determinant), "Invalid determinant %s (%s)"       % (determinant, type(determinant))
        assert isinstance(map_field_methods, dict),  "Invalid map_field_methods %s (%s)" % (map_field_methods, type(map_field_methods))
        for field, methods in map_field_methods.items():
            assert isinstance(field, Field), "Invalid field %s (%s)" % (field, type(field))
            assert isinstance(methods, set), "Invalid methods %s (%s)" % (methods, type(methods))
            for method in methods:
                assert isinstance(method, Method), "Invalid method %s (%s)" % (method, type(method))

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
        """
        \brief Set another Key for this Fd
        \param key The new Key instance
        """
        assert isinstance(key, Key), "Invalid key = %r (%r)" % (key, type(Key))
        self.determinant.set_key(key)

    @returns(set)
    def get_platforms(self):
        """
        \returns A set of strings (the platforms able to provide 
            every fields involved in the key of this fd)
        """
        platforms = set()
        first = True
        for field in self.get_determinant().get_key():
            platforms_cur = set([method.get_platform() for method in self.map_field_methods[field]])
            if first:
                platforms |= platforms_cur 
            else:
                platforms &= platforms_cur 
        return platforms

    @returns(set)
    def get_methods(self):
        """
        \returns The set of Method instances related to this Fd
        """
        ret = set()
        for _, methods in self.get_map_field_methods().items():
            ret |= methods
        return ret

    @returns(Determinant)
    def get_determinant(self):
        """
        \return The Determinant instance related to this Fd
        """
        return self.determinant

    @returns(set)
    def get_fields(self):
        """
        \return The set of output Field instances related to this Fd
        """
        return set(self.map_field_methods.keys())

    @returns(Field)
    def get_field(self):
        """
        \return The Field instance related to this Fd
            (assuming this Fd has exactly one output Field)
        """
        assert len(self.map_field_methods) == 1, "This fd has not exactly one field: %r" % self
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
        cr  = ""
        cr1 = ""
        cr2 = ""
        if len(self.get_fields()) > 1 :
            cr  = "\n"
            cr1 = "\n"
            cr2 = "\n\t"
        
        return "[%s => {%s%s%s}]" % (
            self.get_determinant(),
            cr1,
            cr2.join([
                "%20s\t(via {%s})" % (
                    field,
                    ", ".join(["%r" % method for method in sorted(methods)])
                ) for field, methods in self.map_field_methods.items()
            ]),
            cr
        )

    @returns(str)
    def __repr__(self):
        return "[%r => {%s}]" % (
            self.get_determinant(),
            ', '.join(["%r" % field for field in self.get_fields()])
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
        if (self.get_determinant().get_key() == fd.get_determinant().get_key()) == False:
            raise ValueError("Cannot call |= with parameters (invalid determinants)\n\tself = %r\n\tfd   = %r" % (self, fd))
        for field, methods in fd.map_field_methods.items():
            if field not in self.map_field_methods.keys():
                self.map_field_methods[field] = set()
            self.map_field_methods[field] |= methods
        return self

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
        if not isinstance(fds, (list, set)):
            raise TypeError("Invalid fds %s (type %s)" % (fds, type(fds)))
        for fd in fds:
            if not isinstance(fd, Fd):
                raise TypeError("Invalid fd %s (type %s)" % (fd, type(fd)))

    def __init__(self, fds = set()):
        """
        \brief Constructor of Fds
        \param fds A set or a list of Fd instances
        """
        Fds.check_init(fds)
        # /!\ Don't call set.__init__(fds), copy explicitly each fd 
        for fd in fds:
            self.add(fd)

#OBSOLETE|    #@returns(Fds)
#OBSOLETE|    def collapse(self):
#OBSOLETE|        """
#OBSOLETE|        \brief Aggregate each Fd of this Fds by method name 
#OBSOLETE|        \returns The corresponding Fds instance 
#OBSOLETE|        """
#OBSOLETE|        map_method_fd = dict() 
#OBSOLETE|        for fd in self:
#OBSOLETE|            method = fd.get_determinant().get_method_name()
#OBSOLETE|            if method not in map_method_fd.keys():
#OBSOLETE|                map_method_fd[method] = fd
#OBSOLETE|            else:
#OBSOLETE|                map_method_fd[method] |= fd
#OBSOLETE|
#OBSOLETE|        return Fds(map_method_fd.values())

    @returns(dict)
    def group_by_method(self):
        """
        \brief Group a set of Fd stored in this Fds by method
        \returns A dictionnary {method_name : Fds}
        """
        map_method_fds = dict() 
        for fd in self:
            method = fd.get_determinant().get_method_name()
            if method not in map_method_fds.keys():
                map_method_fds[method] = Fds()
            map_method_fds[method].add(fd)
        return map_method_fds

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

class Cache(dict):
    """
    Cache class
    It stores information concerning the Fd removed from the 3nf graph
    """
    def update(self, fd_removed): 
        """
        \brief Store in the cache that the Fd (key --> field)
            has been removed from the 3nf graph
        \param fd_removed The Fd removed from the 3nf graph 
        """
        assert isinstance(fd_removed, Fd), "Invalid fd = %r (%r)" % (fd, type(fd))
        key = fd_removed.get_determinant().get_key()
        field = fd_removed.get_field()
        methods = fd_removed.get_map_field_methods()[field]

        if key not in self.keys():
            self[key] = dict()
        if field not in self[key].keys():
            self[key][field] = set()

        # Do not add [x --> x] in the self
        if not key.is_composite():
            if key.get_field() == field:
                return 
        self[key][field] |= methods

#====================================================================
# Database normalization
#
# It extends algorithm presented in:
#
#    http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf
#
# In another file, you should only require to import Cache class
# and to_3nf function (and call this function).
#====================================================================

@accepts(set, Fds)
def check_closure(fields, fds):
    """
    \brief (Internal use)
        Check wether paramaters passed to closure()
        are well-formed.
    """
    assert isinstance(fds, Fds),                 "Invalid type of fds (%r)"    % type(fds)
    assert isinstance(fields, (frozenset, set)), "Invalid type of fields (%r)" % type(fields)
    for field in fields:
        assert isinstance(field, Field), "Invalid attribute: type (%r)" % type(field)

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
    check_closure(x, fds)
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

@returns(dict)
@accepts(set, Fds)
def closure_ext(x, fds):
    """
    \brief Compute the closure of a set of attributes under the
        set of functional dependencies
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p7
    \param x A set of Field instances (usually it should be a Key instance) 
    \param fds A Fds instance (each fd must have exactly one output field)
    \return A dictionnary {Field => list(Fd)} where
        - key is a Field in the closure x+
        - data is the sequence of Fd used to retrieve this Field
    """
    # x = source node
    # x+ = vertices reachable from x (at the current iteration)
    # y -> z = an arc (a fd) that is visited during the DFS from (closure of) x

    check_closure(x, fds)
    x_plus_ext = dict()
    for x_elt in x:
        x_plus_ext[x_elt] = list()                 # x+ = x

    print "computing closure with x = %r" % x
    added = True
    while added:                                   # repeat will we visit at least one new fd
        added = False
        for fd in fds:                             #   for each fd (y -> z)
            y = fd.get_determinant().get_key()
            x_plus = set(x_plus_ext.keys())
            if y <= x_plus:                        #     if y in x+
                z = fd.get_field()
                if z not in x_plus:                #       if z not in x+
                    added = True                   #          this fd is relevant, let's visit it
#OBSOLETE|                        x_plus_ext[z] = set()          #          x+ u= y
#OBSOLETE|
#OBSOLETE|                        # "z" is retrieved thanks to "fd" and
#OBSOLETE|                        # each fds needed to retrieve "y"
#OBSOLETE|                        for y_elt in y:
#OBSOLETE|                            if x_plus_ext[y_elt]:
#OBSOLETE|                                x_plus_ext[z] |= x_plus_ext[y_elt]
                    x_plus_ext[z] = list()
                    for y_elt in y:
                        x_plus_ext[z] += x_plus_ext[y_elt]
                    x_plus_ext[z].append(fd)
                    
    for k, d in x_plus_ext.items():
        print "\t%r => %r" % (k,d)
    print "------------"
    return x_plus_ext

@returns(Fds)
@accepts(list)
def make_fd_set(tables):
    """
    \brief Compute the set of functionnal dependancies
    \param A list of input Table instances
    \returns A Fds instance
    """
    fds = Fds() 
    for table in tables:
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
        g2 = Fds([f for f in g if fd != f])             #   g' = g \ {fd}
        x  = fd.get_determinant().get_key()
        a  = fd.get_field()
        x_plus = closure(set(x), g2)                    #   compute x+ according to g'
        if a in x_plus:                                 #   if a \in x+:
            #print "rm %s" % fd
            fds_removed.add(fd)
            g = g2                                      #     g = g'

    for fd in g.copy():                                 # for each fd = [x -> a] in g:
        x = fd.get_determinant().get_key()
        if x.is_composite():                            #   if x has multiple attributes:
            for b in x:                                 #     for each b in x:

                x_b = Key([xi for xi in x if xi != b])  #       x_b = x - b
                g2  = Fds([f for f in g if fd != f])    #       g'  = g \ {fd} \cup {fd'}
                fd2 = copy.deepcopy(fd)                 #          with fd' = [(x - b) -> a]
                fd2.set_key(x_b)
                g2.add(fd2)
                x_b_plus = closure(set(x_b), g2)        #       compute (x - b)+ with repect to g'

                if b in x_b_plus:                       #       if b \subseteq (x - b)+:
                    g = g2                              #         replace [x -> a] in g by [(x - b) -> a]

    return (g, fds_removed) 

@returns(Cache)
@accepts(Fds, Fds)
def reinject_fds(fds_min_cover, fds_removed):
    """
    \brief "Reinject" Fds removed by fd_minimal_cover in the remaining fds.
        Example: P1 provides x -> y, y -> z
                 P2 provides x -> z
                 P3 provides y -> z'
        The min cover is x -> y, y -> z, y -> z' and only the P1 fds are remaining
        Reinjecting "x -> z" in the min cover consist in adding P2 into x -> y and y -> z
            since it is an (arbitrary) path from x to z.
    \param fds_min_cover A Fds instance
    \param fds_removed A Fds instance
    \returns A Cache instance 
    """
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------

    cache = Cache()

    # For each removed Fd [x --> y]
    for fd_removed in fds_removed:

        if fd_removed.get_fields() <= fd_removed.get_determinant().get_key():
            # This includes [x --> x] fd
            print "Reinjecting %s" % fd_removed
            fds_min_cover.add(fd_removed)
        else:
            # (p::m) [x --> y] is a shortcut in the 3nf graph,
            # store (p::m) it in the cache for (x, y) where
            # - p stands for a platform
            # - m stands for a method name ((p::m) is thus a Method instance)
            # - x stands for the Key used by fd_removed
            # - y stands for the output Field of fd_removed
            print "Adding to cache fd_removed = %r" % fd_removed 
            cache.update(fd_removed)
         
    return cache                

@returns(tuple)
@accepts(list)
def to_3nf(tables):
    """
    \brief Compute a 3nf schema
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
    \param A list of Table instances (the schema we want to normalize)
    \return A pair made of
        - the 3nf graph (DbGraph instance)
        - a Cache instance (storing the shortcuts removed from the 3nf graph)
    """
    # Compute functional dependancies
    print "-" * 100
    print "1) Computing functional dependancies"
    print "-" * 100
    fds = make_fd_set(tables)
    #print "%r" % fds

#OBSOLETE|        # Compute the map which refer for each key the platforms
#OBSOLETE|        # which support this key 
#OBSOLETE|        map_key_platforms = dict()
#OBSOLETE|        for fd in fds:
#OBSOLETE|            key = fd.get_determinant().get_key()
#OBSOLETE|            if key not in map_key_platforms.keys():
#OBSOLETE|                map_key_platforms[key] = set()
#OBSOLETE|            for table in tables:
#OBSOLETE|                if table.has_key(key):
#OBSOLETE|                    map_key_platforms[key] |= table.get_platforms()

    # Find a minimal cover
    print "-" * 100
    print "2) Computing minimal cover"
    print "-" * 100
    (fds_min_cover, fds_removed) = fd_minimal_cover(fds)

    print "-" * 100
    print "3) Reinjecting fd removed"
    print "-" * 100
    cache = reinject_fds(fds_min_cover, fds_removed)

    print "-" * 100
    print "4) Grouping fds by method"
    print "-" * 100
    fdss = fds_min_cover.group_by_method()
    #for table_name, fds in fdss.items():
    #    print "%s:\n%s" % (table_name, fds)

    print "-" * 100
    print "5) Making 3-nf tables" 
    print "-" * 100
    tables_3nf = []
    for table_name, fds in fdss.items():
        platforms         = set()
        map_field_methods = dict()
        fields            = set()
        keys              = set()
        for fd in fds:
            keys.add(fd.get_determinant().get_key())
            fields |= fd.get_fields()
            for field, methods in fd.get_map_field_methods().items():
                if field not in map_field_methods:
                    map_field_methods[field] = set()
                map_field_methods[field] |= methods
                for method in methods:
                    platforms.add(method.get_platform())

        table = Table(platforms, map_field_methods, table_name, fields, keys)
        print "%s\n" % table
        tables_3nf.append(table)


    print "-" * 100
    print "6) Building DBgraph"
    print "-" * 100
    graph_3nf = DBGraph(tables_3nf)

    return (graph_3nf, cache)

