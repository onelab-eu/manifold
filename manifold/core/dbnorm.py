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
from manifold.core.table   import Table
from manifold.core.key     import Key, Keys 
from manifold.core.field   import Field
from manifold.core.method  import Method 
from manifold.core.dbgraph import DBGraph
from manifold.util.type    import returns, accepts
from manifold.util.log     import Log

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
        \brief Add for each output Field of this Fd a new Method
        \param method A set of Method instances
        """
        assert isinstance(methods, set), "Invalid methods = %r (%r)" % (methods, type(methods))
        for field in self.map_field_methods.keys():
            self.map_field_methods[field] |= methods

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

    @returns(dict)
    def group_by_method(self):
        """
        \brief Group a set of Fd stored in this Fds by method
        \returns A dictionnary {method_name : Fds}
        """
        map_method_fds = dict() 
        for fd in self:
            _field, _platforms = fd.map_field_methods.items()[0]
            method = fd.get_determinant().get_method_name()
            dict_key = (method, frozenset(_platforms))
            if dict_key not in map_method_fds.keys():
                map_method_fds[dict_key] = Fds()
            map_method_fds[dict_key].add(fd)
        return map_method_fds

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

@accepts(Fds, Fds)
@accepts(Fds, Fds)
def reinject_fds(fds_min_cover, fds_removed):
    """
    \brief Reinject Fds removed by fd_minimal_cover in the remaining fds.
    \param fds_min_cover A Fds instance gatehring the Fd involved in the 3nf graph
    \param fds_removed A Fds instance gathering the Fd instances removed during the normalization.
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

    map_key_closure = {}
    key_fd_by_method = {}
    methods_with_fds = []

    for fd_removed in fds_removed:
        x = fd_removed.get_determinant().get_key()
        y = fd_removed.get_field()
        m = fd_removed.get_methods()

        method = list(m)[0].get_name()

        # (1)
        if y in x: 
            # We cannot simply readd them since it will cause new relations in the 3nf.
            # fds_min_cover.add(fd_removed)
            
            # We have a single method in each fd
            if not method in key_fd_by_method:
                key_fd_by_method[method] = []
            key_fd_by_method[method].append(fd_removed)

            continue

        methods_with_fds.append(method)

        # Compute (if not cached) the underlying 3nf fds allowing to retrieve y from x 
        if x not in map_key_closure.keys():
            map_key_closure[x] = closure_ext(set(x), fds_min_cover) 

        for fd in map_key_closure[x][y]:
            #if (fd.get_determinant().get_key() == x and fd.get_field() in set(x)) or fd.get_field() == y:
            fd.add_methods(m)

    for method, fds in key_fd_by_method.items():
        if not method in methods_with_fds:
            for fd in fds:
                fds_min_cover.add(fd)


def reinject_fds_mando(fds_min_cover, fds_removed):
    """
    \brief "Reinject" Fds removed by fd_minimal_cover in the remaining fds.
    \param fds_min_cover A Fds instance gatehring the Fd involved in the 3nf graph
    \param fds_removed A Fds instance gathering the Fd instances removed during the normalization.
        An Fd in this set is
        - either a Fd [key --> field] such as field in a field involved in Key:
            in this case, we need this Fd to build the appropriate table and
            thus we reinject this Fd in fds_min_cover
        - either a Fd [x --> y] (p::m) since in the min cover it exists a path from x
        to y (for instance x --> a --> b --> y):
            in this case, we add "p::m" to each fd involved in this path, e.g x --> a, a --> b, b --> y
        Example:
            Suppose that p1 announces:
                x { x y }     with KEY(x)
                y { y z }     with KEY(y)
                z { z t }     with KEY(z)
            Suppose that p2 announces:
                x { x y z t } with KEY(x)

            This leads to the following functionnal dependancies:

                x --> x (p1::x)
                x --> y (p1::x)
                y --> z (p1::y)
                z --> t (p1::z)

                x --> x (p2::x)
                x --> y (p2::x)
                x --> z (p2::x)
                x --> t (p2::x)

            => The min_cover is

                x --> y (p1::x and p2::x)
                y --> z (p1::x)
                z --> t (p1::x)

            and removed fd are:

                x --> x (p1::x and p2::x) removed because a key produces always its fields <= WE REINJECT THIS FD IN THIS FUNCTION: see (1)
                x --> z (p2::x)           removed because it exits a "transitive" path (x --> y --> z)
                x --> t (p2::x)           removed because it exits a "transitive" path (x --> y --> z --> t)

            To build our 3nf graph we reinject x --> x in the min_cover because this fd is relevant in our 3nf graph.

            => the 3nf graph will be (see util/DBGraph.py) : (where "*::*" denotes a reinjection)

                x { x y } (via p1::x and p2::x)   with KEY(x)
                y { y z } (via p1::y and "p2::x") with KEY(y)
                z { z t } (via p1::b and "p2::x") with KEY(z)

                with arcs (x --> y) and (y --> z) (feasible joins)

            In other words, the transitive fds x --> z and x --> t provided
            by p2::x are reinjected along the underlying 3nf fds path (resp. x --> y --> z
            and x --> y --> z --> t) <= WE PROVIDE ANNOTATIONS ON THE FIRST AND LAST OF FD OF THESE PATHS (2)

            Note that since this reinjection is made along an 3nf path
            querying p2::x ALWAYS allows to retrieve both the y and z 3nf-tables;
            Indeed we simply have to SELECT the appropriate fields and
            remove the duplicate records according to any key of the 3nf table.

                p2::y <=> DUP_y(SELECT y, z FROM p2::x)
                p2::z <=> DUP_z(SELECT z, t FROM p2::x)
            
            Now, suppose the user queries SELECT y, z FROM y
            The pruned 3nd tree (see util/pruned_tree.py) is rooted on "y" (see FROM y):

                y { y z } (via p1::y and "p2::x")

            => We'll get the query plan (see util/query_plan.py):
                SELECT y, z FROM p1::y
                UNION
                DUP_y(SELECT y, z FROM p2::x)

    \return The remaining removed Fd instances
    """
    fds_remaining = Fds()

    # (1) 
    # Reinject x --> x or x --> x_elt where x_elt is in x
    # (removed during fd_min_cover but required to build the tables)
    # This should be done only if not "onjoin"
    map_key_fdreinjected = dict()
    fd_not_reinjected = Fds()
    for fd_removed in fds_removed:
        x = fd_removed.get_determinant().get_key()
        y = fd_removed.get_field()
        m = fd_removed.get_methods()

        # Is x --> y of type "x --> x" or "x --> x_elt"
        if y in x: 
            # Reinject Fd [key --> field \subseteq key]
            fds_min_cover.add(fd_removed)

##### OBSOLETE <<<
            # Memorize this reinjected Fd in map_key_fdreinjected
            # This will be need in (2)
            if x not in map_key_fdreinjected.keys():
                map_key_fdreinjected[x] = set()
            map_key_fdreinjected[x].add(fd_removed)
##### OBSOLETE >>>
            # Memorize this reinjected Fd in map_key_fdreinjected
            #print "(1) %r" % fd_removed
        else:
            # This fd will be reinjected during (2)
            fd_not_reinjected.add(fd_removed)

    # (2) Reinject shortcut since they may be relevant during the query plane computation
    map_key_closure = dict()
    for fd_removed in fd_not_reinjected:
        x = fd_removed.get_determinant().get_key()
        y = fd_removed.get_field()
        m = fd_removed.get_methods()
        #print "(2) %r" % fd_removed

        # Compute (if not cached) the underlying 3nf fds allowing to retrieve y from x 
        if x not in map_key_closure.keys():
            map_key_closure[x] = closure_ext(set(x), fds_min_cover) 

##### OBSOLETE <<<
        # Reinject removed_fd [x --> y] on its key eg on each [x --> x_elt] fd (share key)
        # These Fds have already been reinjected in (1) and stored in map_key_fdreinjected
        for fd in map_key_fdreinjected[x]:
            #print " >1> ADD METHOD ", m, "TO FD", fd
            if fd.get_determinant().get_method_name() == list(m)[0].get_name():
                # XXX this is not sufficient if the naming is the same
                fd.add_methods(m)
##### OBSOLETE >>>

        # Reinject removed_fd [x --> y] on the first and the last fd of the 3nf path from x to y (share field)
        # A closure stores a set of Fd 
        # - The first fd of this path verifies: fd.get_determinant().get_key() == x and fd.get_field() in set(x)
        # - The last  fd of this path verifies: fd.get_field() == y
        for fd in map_key_closure[x][y]:
            if (fd.get_determinant().get_key() == x and fd.get_field() in set(x)) or fd.get_field() == y:
                #print " >2> ADD METHOD ", m, "TO FD", fd
                fd.add_methods(m)

#@returns(DBGraph)
@accepts(dict)
def to_3nf(metadata):
    """
    \brief Compute a 3nf schema
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
    \param A list of Table instances (the schema we want to normalize)
    \return A pair made of
        - the 3nf graph (DbGraph instance)
        - a Cache instance (storing the shortcuts removed from the 3nf graph)
    """
    # Compute functional dependancies
    #print "-" * 100
    #print "1) Computing functional dependancies"
    #print "-" * 100
    tables = []
    map_method_capabilities = {}
    for platform, announces in metadata.items():
        for announce in announces:
            tables.append(announce.table)
            map_method_capabilities[(platform, announce.table.get_name())] = announce.table.get_capabilities()
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
    #print "-" * 100
    #print "2) Computing minimal cover"
    #print "-" * 100
    (fds_min_cover, fds_removed) = fd_minimal_cover(fds)

    #print "-" * 100
    #print "3) Reinjecting fds removed during normalization"
    #print "-" * 100
    #for fd_removed in fds_removed:
    #    print "(0) %r" % fd_removed

    reinject_fds(fds_min_cover, fds_removed)

    #print "-" * 100
    #print "4) Grouping fds by method"
    #print "-" * 100
    #fdss = fds_min_cover.group_by_method() # Mando
    fdss = fds_min_cover.group_by_tablename_method() # Jordan
    #for table_name, fds in fdss.items():
    #    print "### \t%s:\n%s" % (table_name, fds)

    #print "-" * 100
    #print "5) Making 3-nf tables" 
    #print "-" * 100
    tables_3nf = list()
    map_tablename_methods = dict() # map table_name with methods to demux

    # for table_name_platforms, fds in fdss.items():
    for table_name, map_platform_fds in fdss.items():

        # For the potential parent table
        #

        # Stores the number of distinct platforms set
        cpt_platforms = 0
        # Stores the set of platforms
        all_platforms = set()
        common_fields = None
        common_keys = None
        # Annotations needed for the query plane
        all_tables = []

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

                        # demux annotation
                        method_name = method.get_name()
                        if method_name != table_name :
                            if method_name not in map_tablename_methods.keys():
                                map_tablename_methods[method_name] = set()
                            map_tablename_methods[method_name].add(method)

                        platforms.add(method.get_platform())

            table = Table(platforms, None, table_name, fields, keys)

            # inject field and key annotation in the Table object
            table.map_method_keys   = map_method_keys
            table.map_method_fields = map_method_fields
            tables_3nf.append(table)
            all_tables.append(table)
            Log.debug("TABLE 3nf:", table, table.keys)
            #print "     method fields", map_method_fields

            cpt_platforms += 1
            all_platforms |= platforms
            if not common_fields:
                common_fields = fields
            else:
                common_fields &= fields

            if not common_keys:
                common_keys = keys
            else:
                common_keys &= keys


        # Need to add a parent table if more than two sets of platforms
        if cpt_platforms > 1:
            table = Table(all_platforms, None, table_name, common_fields, common_keys)

            # Migrate common fields from children to parents, except keys
            ##map_common_method_keys   = dict()
            map_common_method_fields = dict()

            for field in common_fields:
                methods = set()
                for child_table in all_tables:
                    # Objective = remove the field from child table
                    # Several methods can have it
                    for _method, _fields in child_table.map_method_fields.items():
                        if field.get_name() in _fields:
                            methods.add(_method)
                            if not common_keys.has_field(field):
                                _fields.remove(field.get_name())

                if not common_keys.has_field(field):
                    del child_table.fields[field.get_name()]
                # Add the field with all methods to parent table
                for method in methods:
                    if not method in map_common_method_fields: map_common_method_fields[method] = set()
                    map_common_method_fields[method].add(field.get_name())

            map_common_method_fields[method].add(field.get_name())

            # inject field and key annotation in the Table object
            table.map_method_keys   = dict() #map_common_method_keys
            table.map_method_fields = map_common_method_fields
            tables_3nf.append(table)
            Log.debug("TABLE 3nf:", table, table.keys)
            #print "     method fields", map_common_method_fields

        # XXX we already know about the links between those two platforms
        # but we can find them easily (cf dbgraph)

    # inject demux annotation
    for table in tables_3nf:
        if table.get_name() in map_tablename_methods.keys():
            table.methods_demux = map_tablename_methods[table.get_name()]
        else:
            table.methods_demux = set()

    #print "-" * 100
    #print "6) Building DBgraph"
    #print "-" * 100
    # TODO: capabilities are now in tables, shall they be present in tables_3nf
    # instead of relying on map_method_capabilities ?
    graph_3nf = DBGraph(tables_3nf, map_method_capabilities)

    return graph_3nf

