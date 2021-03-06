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

import sys, copy
from types                 import StringTypes

from manifold.core.dbgraph import DBGraph
from manifold.core.field   import Field
from manifold.core.fields  import Fields
from manifold.core.key     import Key, Keys
from manifold.core.method  import Method 
from manifold.core.table   import Table
from manifold.util.log     import Log
from manifold.util.type    import returns, accepts

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

    @returns(StringTypes)
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

    @returns(StringTypes)
    def __str__(self):
        """
        \return The (verbose) string representing a Determinant instance
        """
        return self.__repr__()

    @returns(StringTypes)
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
        \param map_field_methods A dictionnary {Field => {Methods} }
            storing for each field the corresponding methods
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
        for methods in self.get_map_field_methods().values():
            ret |= methods
        return ret

    @returns(Determinant)
    def get_determinant(self):
        """
        Returns:
            The Determinant instance related to this Fd
        """
        return self.determinant

    @returns(set)
    def get_fields(self):
        """
        Returns:
            The set of output Field instances related to this Fd
        """
        return set(self.map_field_methods.keys())

    @returns(Field)
    def get_field(self):
        """
        Returns:
            The Field instance related to this Fd
            (assuming this Fd has exactly one output Field)
        """
        assert len(self.map_field_methods) == 1, "This fd has not exactly one field: %r" % self
        for field in self.map_field_methods.keys():
            return field

    @returns(set)
    def split(self):
        """
        Split a fd
            Example: [k -> {f1, f2...}] is split into {[k -> f1], [k -> f2], ...}
        Returns:
            A set of Fd instances
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

    #@returns(StringTypes)
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

    @returns(StringTypes)
    def __repr__(self):
        return "[%r => {%s}]" % (
            self.get_determinant(),
            ', '.join(["%r" % field for field in self.get_fields()])
        )

    @returns(dict)
    def get_map_field_methods(self):
        return self.map_field_methods

    def add_methods(self, methods):
        """
        Add for each output Field of this Fd a new Method
        Args:
            method: A set of Method instances
        """
        assert isinstance(methods, set), "Invalid methods = %r (%r)" % (methods, type(methods))

        for field in self.map_field_methods.keys():
            self.map_field_methods[field] |= methods

    def __ior__(self, fd):
        """
        |= overloading
        Args:
             fd: A Fd instance that we merge with self
        Returns:
            self
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

#OBSOLETE|    @returns(dict)
#OBSOLETE|    def group_by_method(self):
#OBSOLETE|        """
#OBSOLETE|        \brief Group a set of Fd stored in this Fds by method
#OBSOLETE|        \returns A dictionnary {method_name : Fds}
#OBSOLETE|        """
#OBSOLETE|        map_method_fds = dict() 
#OBSOLETE|        for fd in self:
#OBSOLETE|            _field, _platforms = fd.map_field_methods.items()[0]
#OBSOLETE|            method = fd.get_determinant().get_method_name()
#OBSOLETE|            dict_key = (method, frozenset(_platforms))
#OBSOLETE|            if dict_key not in map_method_fds.keys():
#OBSOLETE|                map_method_fds[dict_key] = Fds()
#OBSOLETE|            map_method_fds[dict_key].add(fd)
#OBSOLETE|        return map_method_fds

    # Replaces the previous function
    @returns(dict)
    def group_by_tablename_method(self):
        """
        \brief Group a set of Fd stored in this Fds by tablename then method
        \returns A dictionnary {method_name : Fds}
        """
        map_method_fds = {}
        for fd in self:
            _field, _platforms = fd.map_field_methods.items()[0]
            _platforms = frozenset(_platforms)

            method = fd.get_determinant().get_method_name()
            if method not in map_method_fds:
                 map_method_fds[method] = {}
            if _platforms not in map_method_fds[method]:
                map_method_fds[method][_platforms] = Fds()
            map_method_fds[method][_platforms].add(fd)
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

    @returns(StringTypes)
    def __str__(self):
        return '\n'.join(["%s" % fd for fd in self])

    @returns(StringTypes)
    def __repr__(self):
        return '\n'.join(["%r" % fd for fd in self])

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
        - data is a set of Fd needed to retrieve this Field
    """
    # x = source node
    # x+ = vertices reachable from x (at the current iteration)
    # y -> z = an arc (a fd) that is visited during the DFS from (closure of) x

    check_closure(x, fds)
    x_plus_ext = dict()
    for x_elt in x:
        x_plus_ext[x_elt] = set()                 # x+ = x

    #print "computing closure with x = %r and fds = %r" % (x, fds)
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
                    x_plus_ext[z] = set()          #          x+ u= y

                    # "z" is retrieved thanks to
                    #  - each Fd needed to retrieve "y"
                    #  - the Fd [y --> z] 
                    for y_elt in y:
                        if x_plus_ext[y_elt]:
                            x_plus_ext[z] |= x_plus_ext[y_elt]
                    x_plus_ext[z].add(fd)

    return x_plus_ext

@returns(Fds)
@accepts(list)
def make_fd_set(tables):
    """
    Compute the set of functionnal dependancies.
    Args:
        tables: A list of input Table instances.
    Returns:
        A Fds instance.
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
    Compute the functionnal dependancy minimal cover
        See http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p11
    Args:
        fds: A Fds instance 
    Returns:
        A couple made of:
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

@accepts(Fds, Fds)
def reinject_fds(fds_min_cover, fds_removed):
    """
    Reinject Fds removed by fd_minimal_cover in the remaining fds.
    Args:
        fds_min_cover: A Fds instance gatehring the Fd involved in the 3nf graph. This
            parameter will be enriched with some previously removed Fd instances.
        fds_removed: A Fds instance gathering the Fd instances removed during the normalization.
            An Fd in this set is:
            (1) either a Fd [x --> x]
                or a Fd [x --> x_i] with x = (x_0, ..., x_N)
                (We ignore such Fds and will take care of keys when building final tables)
            (2) either a Fd [x --> y] which is redundant with an existing Fd
                (We reinject the information in the corresponding Fd)
            (3) either a Fd [x --> z] which is a transitive Fds such as x --> ... --> z
                (We reinject the information along the whole path)

            Both last cases can be treated in the same way
    """

    map_key_closure  = dict() # Cache (it maps each table and its corresponding closure, e.g. { x : x+ })
    key_fd_by_method = dict() # Maps for each method (i.e. (table, key) pair) which Fds must be reinjected: { method : {fds_removed} }
    methods_with_fds = list() # Stores which methods are altered during the (2) and (3) reinjection phases

    for fd_removed in fds_removed:
        x = fd_removed.get_determinant().get_key()
        y = fd_removed.get_field()
        m = fd_removed.get_methods()

        method = list(m)[0].get_name()

        # (1)
        if y in x: 
            # We cannot simply re-add them since it will cause new relations in the 3nf.
            # fds_min_cover.add(fd_removed)
            
            # We have a single method in each fd
            if not method in key_fd_by_method:
                key_fd_by_method[method] = list()
            key_fd_by_method[method].append(fd_removed)

            continue

        # (2)+(3)
        methods_with_fds.append(method)

        # Compute (if not cached) the underlying 3nf fds allowing to retrieve y from x 
        if x not in map_key_closure.keys():
            map_key_closure[x] = closure_ext(set(x), fds_min_cover) 

        # (2)
        for fd in map_key_closure[x][y]:
            #if (fd.get_determinant().get_key() == x and fd.get_field() in set(x)) or fd.get_field() == y:
            fd.add_methods(m)

    for method, fds in key_fd_by_method.items():
        if not method in methods_with_fds:
            for fd in fds:
                fds_min_cover.add(fd)

#UNUSED|def reinject_fds_mando(fds_min_cover, fds_removed):
#UNUSED|    """
#UNUSED|    \brief "Reinject" Fds removed by fd_minimal_cover in the remaining fds.
#UNUSED|    \param fds_min_cover A Fds instance gatehring the Fd involved in the 3nf graph
#UNUSED|    \param fds_removed A Fds instance gathering the Fd instances removed during the normalization.
#UNUSED|        An Fd in this set is
#UNUSED|        - either a Fd [key --> field] such as field in a field involved in Key:
#UNUSED|            in this case, we need this Fd to build the appropriate table and
#UNUSED|            thus we reinject this Fd in fds_min_cover
#UNUSED|        - either a Fd [x --> y] (p::m) since in the min cover it exists a path from x
#UNUSED|        to y (for instance x --> a --> b --> y):
#UNUSED|            in this case, we add "p::m" to each fd involved in this path, e.g x --> a, a --> b, b --> y
#UNUSED|        Example:
#UNUSED|            Suppose that p1 announces:
#UNUSED|                x { x y }     with KEY(x)
#UNUSED|                y { y z }     with KEY(y)
#UNUSED|                z { z t }     with KEY(z)
#UNUSED|            Suppose that p2 announces:
#UNUSED|                x { x y z t } with KEY(x)
#UNUSED|
#UNUSED|            This leads to the following functionnal dependancies:
#UNUSED|
#UNUSED|                x --> x (p1::x)
#UNUSED|                x --> y (p1::x)
#UNUSED|                y --> z (p1::y)
#UNUSED|                z --> t (p1::z)
#UNUSED|
#UNUSED|                x --> x (p2::x)
#UNUSED|                x --> y (p2::x)
#UNUSED|                x --> z (p2::x)
#UNUSED|                x --> t (p2::x)
#UNUSED|
#UNUSED|            => The min_cover is
#UNUSED|
#UNUSED|                x --> y (p1::x and p2::x)
#UNUSED|                y --> z (p1::x)
#UNUSED|                z --> t (p1::x)
#UNUSED|
#UNUSED|            and removed fd are:
#UNUSED|
#UNUSED|                x --> x (p1::x and p2::x) removed because a key produces always its fields <= WE REINJECT THIS FD IN THIS FUNCTION: see (1)
#UNUSED|                x --> z (p2::x)           removed because it exits a "transitive" path (x --> y --> z)
#UNUSED|                x --> t (p2::x)           removed because it exits a "transitive" path (x --> y --> z --> t)
#UNUSED|
#UNUSED|            To build our 3nf graph we reinject x --> x in the min_cover because this fd is relevant in our 3nf graph.
#UNUSED|
#UNUSED|            => the 3nf graph will be (see util/DBGraph.py) : (where "*::*" denotes a reinjection)
#UNUSED|
#UNUSED|                x { x y } (via p1::x and p2::x)   with KEY(x)
#UNUSED|                y { y z } (via p1::y and "p2::x") with KEY(y)
#UNUSED|                z { z t } (via p1::b and "p2::x") with KEY(z)
#UNUSED|
#UNUSED|                with arcs (x --> y) and (y --> z) (feasible joins)
#UNUSED|
#UNUSED|            In other words, the transitive fds x --> z and x --> t provided
#UNUSED|            by p2::x are reinjected along the underlying 3nf fds path (resp. x --> y --> z
#UNUSED|            and x --> y --> z --> t) <= WE PROVIDE ANNOTATIONS ON THE FIRST AND LAST OF FD OF THESE PATHS (2)
#UNUSED|
#UNUSED|            Note that since this reinjection is made along an 3nf path
#UNUSED|            querying p2::x ALWAYS allows to retrieve both the y and z 3nf-tables;
#UNUSED|            Indeed we simply have to SELECT the appropriate fields and
#UNUSED|            remove the duplicate records according to any key of the 3nf table.
#UNUSED|
#UNUSED|                p2::y <=> DUP_y(SELECT y, z FROM p2::x)
#UNUSED|                p2::z <=> DUP_z(SELECT z, t FROM p2::x)
#UNUSED|            
#UNUSED|            Now, suppose the user queries SELECT y, z FROM y
#UNUSED|            The pruned 3nd tree (see util/pruned_tree.py) is rooted on "y" (see FROM y):
#UNUSED|
#UNUSED|                y { y z } (via p1::y and "p2::x")
#UNUSED|
#UNUSED|            => We'll get the query plan (see util/query_plan.py):
#UNUSED|                SELECT y, z FROM p1::y
#UNUSED|                UNION
#UNUSED|                DUP_y(SELECT y, z FROM p2::x)
#UNUSED|
#UNUSED|    \return The remaining removed Fd instances
#UNUSED|    """
#UNUSED|    fds_remaining = Fds()
#UNUSED|
#UNUSED|    # (1) 
#UNUSED|    # Reinject x --> x or x --> x_elt where x_elt is in x
#UNUSED|    # (removed during fd_min_cover but required to build the tables)
#UNUSED|    # This should be done only if not "onjoin"
#UNUSED|    map_key_fdreinjected = dict()
#UNUSED|    fd_not_reinjected = Fds()
#UNUSED|    for fd_removed in fds_removed:
#UNUSED|        x = fd_removed.get_determinant().get_key()
#UNUSED|        y = fd_removed.get_field()
#UNUSED|        m = fd_removed.get_methods()
#UNUSED|
#UNUSED|        # Is x --> y of type "x --> x" or "x --> x_elt"
#UNUSED|        if y in x: 
#UNUSED|            # Reinject Fd [key --> field \subseteq key]
#UNUSED|            fds_min_cover.add(fd_removed)
#UNUSED|
#UNUSED|##### OBSOLETE <<<
#UNUSED|            # Memorize this reinjected Fd in map_key_fdreinjected
#UNUSED|            # This will be need in (2)
#UNUSED|            if x not in map_key_fdreinjected.keys():
#UNUSED|                map_key_fdreinjected[x] = set()
#UNUSED|            map_key_fdreinjected[x].add(fd_removed)
#UNUSED|##### OBSOLETE >>>
#UNUSED|            # Memorize this reinjected Fd in map_key_fdreinjected
#UNUSED|            #print "(1) %r" % fd_removed
#UNUSED|        else:
#UNUSED|            # This fd will be reinjected during (2)
#UNUSED|            fd_not_reinjected.add(fd_removed)
#UNUSED|
#UNUSED|    # (2) Reinject shortcut since they may be relevant during the query plane computation
#UNUSED|    map_key_closure = dict()
#UNUSED|    for fd_removed in fd_not_reinjected:
#UNUSED|        x = fd_removed.get_determinant().get_key()
#UNUSED|        y = fd_removed.get_field()
#UNUSED|        m = fd_removed.get_methods()
#UNUSED|        #print "(2) %r" % fd_removed
#UNUSED|
#UNUSED|        # Compute (if not cached) the underlying 3nf fds allowing to retrieve y from x 
#UNUSED|        if x not in map_key_closure.keys():
#UNUSED|            map_key_closure[x] = closure_ext(set(x), fds_min_cover) 
#UNUSED|
#UNUSED|##### OBSOLETE <<<
#UNUSED|        # Reinject removed_fd [x --> y] on its key eg on each [x --> x_elt] fd (share key)
#UNUSED|        # These Fds have already been reinjected in (1) and stored in map_key_fdreinjected
#UNUSED|        for fd in map_key_fdreinjected[x]:
#UNUSED|            #print " >1> ADD METHOD ", m, "TO FD", fd
#UNUSED|            if fd.get_determinant().get_method_name() == list(m)[0].get_name():
#UNUSED|                # XXX this is not sufficient if the naming is the same
#UNUSED|                fd.add_methods(m)
#UNUSED|##### OBSOLETE >>>
#UNUSED|
#UNUSED|        # Reinject removed_fd [x --> y] on the first and the last fd of the 3nf path from x to y (share field)
#UNUSED|        # A closure stores a set of Fd 
#UNUSED|        # - The first fd of this path verifies: fd.get_determinant().get_key() == x and fd.get_field() in set(x)
#UNUSED|        # - The last  fd of this path verifies: fd.get_field() == y
#UNUSED|        for fd in map_key_closure[x][y]:
#UNUSED|            if (fd.get_determinant().get_key() == x and fd.get_field() in set(x)) or fd.get_field() == y:
#UNUSED|                #print " >2> ADD METHOD ", m, "TO FD", fd
#UNUSED|                fd.add_methods(m)

#@returns(DBGraph)
#TODO to_3nf should not consider a list of Announces!
@accepts(dict)
def to_3nf(metadata):
    """
    Compute a 3nf schema
    See also http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
    Args:
        metadata: A dictionnary {String => list(Announces)} which maps
            platform name a list containing its corresponding Announces.
    Returns:
        The corresponding 3nf graph (DbGraph instance)
    """
    # 1) Compute functional dependancies
    tables = []
    map_method_capabilities = {}
    for platform, announces in metadata.items():
        for announce in announces:
            tables.append(announce.table)
            map_method_capabilities[(platform, announce.table.get_name())] = announce.table.get_capabilities()
    fds = make_fd_set(tables)

    # 2) Find a minimal cover
    (fds_min_cover, fds_removed) = fd_minimal_cover(fds)

    # 3) Reinjecting fds removed during normalization
    reinject_fds(fds_min_cover, fds_removed)

    # 4) Grouping fds by method
#OBOSOLETE|    fdss = fds_min_cover.group_by_method() # Mando
    fdss = fds_min_cover.group_by_tablename_method() # Jordan

    # 5) Making 3-nf tables
    tables_3nf = list()
#DEPRECATED|LOIC|    map_tablename_methods = dict() # map table_name with methods to demux
#DEPRECATED|LOIC|
    for table_name, map_platform_fds in fdss.items():
        # For the potential parent table
        # Stores the number of distinct platforms set
        num_platforms = 0

        # Stores the set of platforms
        all_platforms = set()
        common_fields = Fields()
        common_key_names = set()

        # Annotations needed for the query plan
        child_tables = list()

        for platform, fds in map_platform_fds.items():
            platforms         = set()
            fields            = set()
            keys              = Keys()

            # Annotations needed for the query plane
            map_method_keys   = dict()
            map_method_fields = dict()

            for fd in fds:
                key = fd.get_determinant().get_key()
                keys.add(key)
                fields |= fd.get_fields()
                
                # We need to add fields from the key
                for key_field in key:
                    fields.add(key_field) # XXX

                for field, methods in fd.get_map_field_methods().items():

                    for method in methods:

                        # key annotation
                        if not method in map_method_keys.keys():
                            map_method_keys[method] = set()
                        map_method_keys[method].add(key)

                        # field annotations
                        if not method in map_method_fields.keys():
                            map_method_fields[method] = set()
                        map_method_fields[method].add(field.get_name())
                        map_method_fields[method].add(key_field.get_name())

#DEPRECATED|LOIC|                        # demux annotation
#DEPRECATED|LOIC|                        method_name = method.get_name()
#DEPRECATED|LOIC|                        if method_name != table_name :
#DEPRECATED|LOIC|                            if method_name not in map_tablename_methods.keys():
#DEPRECATED|LOIC|                                map_tablename_methods[method_name] = set()
#DEPRECATED|LOIC|                            map_tablename_methods[method_name].add(method)
#DEPRECATED|LOIC|
                        platforms.add(method.get_platform())

            table = Table(platforms, None, table_name, fields, keys)

            # inject field and key annotation in the Table object
            table.map_method_keys   = map_method_keys
            table.map_method_fields = map_method_fields
            tables_3nf.append(table)
            child_tables.append(table)
            Log.debug("TABLE 3nf:", table, table.keys)
            #print "     method fields", map_method_fields

            num_platforms += 1
            all_platforms |= platforms
            if common_fields.is_empty():
                common_fields = Fields(fields)
            else:
                common_fields &= Fields(fields)

            keys_names = frozenset([field.get_name() for field in key for key in keys])
            common_key_names.add(keys_names)

        # Convert common_key_names into Keys() according to common_fields
        common_keys = set()
        map_name_fields = dict()
        for field in common_fields:
            map_name_fields[field.get_name()] = field
        for key_names in common_key_names:
            common_keys.add(Key(frozenset([map_name_fields[field_name] for field_name in key_names])))

        # Several platforms provide the same object, so we've to build a parent table
        if num_platforms > 1:
            parent_table = Table(all_platforms, None, table_name, common_fields, common_keys)

            # Migrate common fields from children to parents, except keys
            parent_map_method_fields = dict()
            names_in_common_keys = key.get_field_names()

            for field in common_fields:
                methods = set()
                field_name = field.get_name()
                for child_table in child_tables:
                    # Objective = remove the field from child table
                    # Several methods can have it
                    for _method, _fields in child_table.map_method_fields.items():
                        if field_name in _fields:
                            methods.add(_method)
                            if field_name not in names_in_common_keys:
                                _fields.remove(field.get_name())

                if field_name not in names_in_common_keys:
                    child_table.erase_field(field_name)

                # Add the field with all methods to parent_table
                for method in methods:
                    if not method in parent_map_method_fields: parent_map_method_fields[method] = set()
                    parent_map_method_fields[method].add(field.get_name())

            #MANDO|parent_map_method_fields[method].add(field.get_name())

            # inject field and key annotation in the Table object
#MANDO|DEPRECATED|            parent_table.map_method_keys   = dict() #map_common_method_keys
            parent_table.map_method_fields = parent_map_method_fields
            tables_3nf.append(parent_table)
            Log.debug("Parent table TABLE 3nf:", parent_table, table.get_keys())
            #print "     method fields", parent_map_method_fields

        # XXX we already know about the links between those two platforms
        # but we can find them easily (cf dbgraph)

#DEPRECATED|LOIC|    # inject demux annotation
#DEPRECATED|LOIC|    for table in tables_3nf:
#DEPRECATED|LOIC|        if table.get_name() in map_tablename_methods.keys():
#DEPRECATED|LOIC|            table.methods_demux = map_tablename_methods[table.get_name()]
#DEPRECATED|LOIC|        else:
#DEPRECATED|LOIC|            table.methods_demux = set()

    # 6) Inject capabilities
    # TODO: capabilities are now in tables, shall they be present in tables_3nf
    # instead of relying on map_method_capabilities ?
    for table in tables_3nf:
        for announces in metadata.values():
            for announce in announces:
                if announce.get_table().get_name() == table.get_name():
                    capabilities = table.get_capabilities()
                    if capabilities.is_empty():
                        table.set_capability(announce.get_table().get_capabilities()) 
                    elif not capabilities == announce.get_table().get_capabilities():
                        Log.warning("Conflicting capabilities for tables %r (%r) and %r (%r)" % (
                            table,
                            capabilities,
                            announce.get_table(),
                            announce.get_table().get_capabilities()
                        ))
                
    # 7) Building DBgraph
    graph_3nf = DBGraph(tables_3nf, map_method_capabilities)

    for table in tables_3nf:
        Log.info("%s" % table)
    return graph_3nf

