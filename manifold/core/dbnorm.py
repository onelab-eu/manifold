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
from types                      import StringTypes

from manifold.core.capabilities import Capabilities, merge_capabilities
#DEPRECATED|from manifold.core.dbgraph      import DBGraph
from manifold.core.field        import Field, merge_fields
from manifold.core.field_names  import FieldNames
from manifold.core.key          import Key
from manifold.core.keys         import Keys
from manifold.core.method       import Method
from manifold.core.table        import Table
from manifold.util.log          import Log
from manifold.util.type         import returns, accepts

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
        (Internal use)
        Check whether parameters passed to __init__ are well-formed
        """
        assert isinstance(key, Key),                 "Invalid key %r (type = %r)" % (key, type(key))
        assert isinstance(method_name, StringTypes), "Invalid method_name %r (type = %r)" % (method_name, type(method_name))

    def __init__(self, key, method_name):
        """
        Constructor of Determinant
        Args:
            key: The Key of the Determinant (k)
            method_name: A String corresponding to the name of the Method (m)
        """
        Determinant.check_init(key, method_name)
        self.key = key
        self.method_name = method_name

    def set_key(self, key):
        """
        Set the Key related to this Determinant
        Args:
            key: A Key instance.
        """
        assert isinstance(key, Key), "Invalid key %s (%s)" % (key, type(key))
        self.key = key

    @returns(Key)
    def get_key(self):
        """
        Returns:
            The Key instance related to this determinant (k)
        """
        return self.key

    @returns(StringTypes)
    def get_method_name(self):
        """
        Returns:
            The String corresponding to the method name of this Determinant.
        """
        return self.method_name

    def __hash__(self):
        """
        Returns:
            The hash of this Determinant
        """
        return hash(self.get_key())

    @returns(bool)
    def __eq__(self, x):
        """
        Compare two Determinant instances.
        Args:
            x: The Determinant instance compared to "self"
        Returns:
            True iif self == x
        """
        assert isinstance(x, Determinant), "Invalid paramater %r (type %r)" % (x, type(x))
        return self.get_key() == x.get_key() and self.get_method_name() == x.get_method_name()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The String corresponding to this Determinant instance.
        """
        return self.__repr__()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The String representing this Determinant instance.
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
        Args:
            determinant: A Determinant instance.
            map_field_methods: A {Field : set(Method)} instance
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
        for field in self.get_determinant().get_key().get_fields():
            platforms_cur = set([method.get_platform() for method in self.map_field_methods[field]])
            if first:
                platforms |= platforms_cur
            else:
                platforms &= platforms_cur
        return platforms

    @returns(set)
    def get_methods(self):
        """
        Returns:
            The set of Method instances related to this Fd
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

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Fd instance.
        """
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
        """
        Returns:
            The '%r' representation of this Fd instance.
        """
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
        if (self.get_determinant().get_key().get_fields() == fd.get_determinant().get_key().get_fields()) == False:
            raise ValueError("Cannot call |= with parameters (invalid determinants)\n\tself = %r\n\tfd   = %r" % (self, fd))
        for field, methods in fd.map_field_methods.items():
            if field not in self.map_field_methods.keys():
                self.map_field_methods[field] = set()
            self.map_field_methods[field] |= methods
        return self

    def copy(self):
        return copy.deepcopy(self)

#------------------------------------------------------------------------------

class Fds(set):
    """
    Functionnal dependancies.
    """
    @staticmethod
    def check_init(fds):
        """
        (Internal use)
        Args:
            fds: A set of Fd instances
        """
        if not isinstance(fds, (list, set)):
            raise TypeError("Invalid fds %s (type %s)" % (fds, type(fds)))
        for fd in fds:
            if not isinstance(fd, Fd):
                raise TypeError("Invalid fd %s (type %s)" % (fd, type(fd)))

    def __init__(self, fds = set()):
        """
        Constructor of Fds.
        Args:
            fds: A set or a list of Fd instances
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
        Group a set of Fd stored in this Fds by tablename then method
        Returns:
            A dictionnary {table_name (String) : {platform_name (String) : Fds} }
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
        Split a Fds instance.
        Returns:
            The corresponding Fds instance
        """
        fds = Fds()
        for fd in self:
            fds |= fd.split()
        return fds

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Fds instance.
        """
        return "\n".join(["%s" % fd for fd in self])

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Fds instance.
        """
        return "\n".join(["%r" % fd for fd in self])

    def copy(self):
        return copy.deepcopy(self)

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

@accepts(frozenset, Fds)
def check_closure(fields, fds):
    """
    (Internal use)
    Check wether paramaters passed to closure()
    are well-formed.
    """
    assert isinstance(fds, Fds),                 "Invalid type of fds (%r)"    % type(fds)
    assert isinstance(fields, (frozenset, set)), "Invalid type of fields (%r)" % type(fields)
    for field in fields:
        assert isinstance(field, Field), "Invalid attribute: type (%r)" % type(field)

@accepts(frozenset, Fds)
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
            if key.get_fields() <= x_plus:                    #     if y \subseteq x+
                x_plus |= fd.get_fields()        #       x+ = x+ \cup z
        if old_x_plus == x_plus:                 # until temp_x+ = x+
            break
    return x_plus

@returns(dict)
@accepts(frozenset, Fds)
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
            if y.get_fields() <= x_plus:                        #     if y in x+
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
        x  = fd.get_determinant().get_key().get_fields()
        a  = fd.get_field()
        x_plus = closure(x, g2)                    #   compute x+ according to g'
        if a in x_plus:                                 #   if a \in x+:
            #print "rm %s" % fd
            fds_removed.add(fd)
            g = g2                                      #     g = g'

    for fd in g.copy():                                 # for each fd = [x -> a] in g:
        x = fd.get_determinant().get_key()
        if x.is_composite():                            #   if x has multiple attributes:
            for b in x.get_fields():                    #     for each b in x:

                x_b = Key([xi for xi in x.get_fields() if xi != b])  #       x_b = x - b
                g2  = Fds([f for f in g if fd != f])    #       g'  = g \ {fd} \cup {fd'}
                fd2 = copy.deepcopy(fd)                 #          with fd' = [(x - b) -> a]
                fd2.set_key(x_b)
                g2.add(fd2)
                x_b_plus = closure(x_b.get_fields(), g2)        #       compute (x - b)+ with repect to g'

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
            map_key_closure[x] = closure_ext(x.get_fields(), fds_min_cover)

        # (2)
        for fd in map_key_closure[x][y]:
            #if (fd.get_determinant().get_key() == x and fd.get_field() in set(x)) or fd.get_field() == y:
            fd.add_methods(m)

    for method, fds in key_fd_by_method.items():
        if not method in methods_with_fds:
            for fd in fds:
                fds_min_cover.add(fd)

#DEPRECATED|@returns(DBGraph)
#DEPRECATED|#TODO to_3nf should not consider a list of Announces!
#DEPRECATED|@accepts(dict)
#DEPRECATED|def to_3nf(metadata):
#DEPRECATED|    """
#DEPRECATED|    Compute a 3nf schema
#DEPRECATED|    See also http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
#DEPRECATED|    Args:
#DEPRECATED|        metadata: A dictionnary {String => list(Announces)} which maps
#DEPRECATED|            platform name a list containing its corresponding Announces.
#DEPRECATED|    Returns:
#DEPRECATED|        The corresponding 3nf graph (DbGraph instance)
#DEPRECATED|    """
#DEPRECATED|    # 1) Compute functional dependancies
#DEPRECATED|    tables = list()
#DEPRECATED|    local_tables = list()
#DEPRECATED|    map_method_capabilities = dict()
#DEPRECATED|    for platform, announces in metadata.items():
#DEPRECATED|        for announce in announces:
#DEPRECATED|            table = announce.get_table()
#DEPRECATED|            # XXX local aspect is local during normalization, unless we store it
#DEPRECATED|            # in FD
#DEPRECATED|            if table.keys.is_local(): # and not table.keys:
#DEPRECATED|                local_tables.append(table)
#DEPRECATED|            else:
#DEPRECATED|                tables.append(table)
#DEPRECATED|            map_method_capabilities[Method(platform, table.get_name())] = table.get_capabilities()
#DEPRECATED|    fds = make_fd_set(tables)
#DEPRECATED|
#DEPRECATED|    # 2) Find a minimal cover
#DEPRECATED|    (fds_min_cover, fds_removed) = fd_minimal_cover(fds)
#DEPRECATED|
#DEPRECATED|    # 3) Reinjecting fds removed during normalization
#DEPRECATED|    reinject_fds(fds_min_cover, fds_removed)
#DEPRECATED|
#DEPRECATED|    # 4) Grouping fds by method
#DEPRECATED|#OBOSOLETE|    fdss = fds_min_cover.group_by_method() # Mando
#DEPRECATED|    fdss = fds_min_cover.group_by_tablename_method() # Jordan
#DEPRECATED|
#DEPRECATED|    # 5) Making 3-nf tables
#DEPRECATED|    tables_3nf = list()
#DEPRECATED|    map_tablename_methods = dict() # map table_name with methods to demux
#DEPRECATED|
#DEPRECATED|    for table_name, map_platform_fds in fdss.items():
#DEPRECATED|        # For the potential parent table
#DEPRECATED|        # Stores the number of distinct platforms set
#DEPRECATED|        num_platforms = 0
#DEPRECATED|
#DEPRECATED|        # Stores the set of platforms
#DEPRECATED|        all_platforms = set()
#DEPRECATED|        common_fields = None
#DEPRECATED|        common_keys = None
#DEPRECATED|        all_keys = Keys()
#DEPRECATED|
#DEPRECATED|        # Annotations needed for the query plane
#DEPRECATED|        all_tables = list()
#DEPRECATED|
#DEPRECATED|        for platform, fds in map_platform_fds.items():
#DEPRECATED|            platforms = set()
#DEPRECATED|            fields    = set()
#DEPRECATED|            keys      = Keys()
#DEPRECATED|
#DEPRECATED|            # Annotations needed for the query plane
#DEPRECATED|            map_method_keys   = dict()
#DEPRECATED|            map_method_fieldnames = dict()
#DEPRECATED|
#DEPRECATED|            for fd in fds:
#DEPRECATED|                key = fd.get_determinant().get_key()
#DEPRECATED|                keys.add(key)
#DEPRECATED|                fields |= fd.get_fields()
#DEPRECATED|
#DEPRECATED|                # We need to add fields from the key
#DEPRECATED|                for key_field in key:
#DEPRECATED|                    fields.add(key_field) # XXX
#DEPRECATED|
#DEPRECATED|                for field, methods in fd.get_map_field_methods().items():
#DEPRECATED|
#DEPRECATED|                    for method in methods:
#DEPRECATED|
#DEPRECATED|                        # key annotation
#DEPRECATED|                        if not method in map_method_keys.keys():
#DEPRECATED|                            map_method_keys[method] = set()
#DEPRECATED|                        map_method_keys[method].add(key)
#DEPRECATED|
#DEPRECATED|                        # field annotation
#DEPRECATED|                        if not method in map_method_fieldnames.keys():
#DEPRECATED|                            map_method_fieldnames[method] = set()
#DEPRECATED|                        map_method_fieldnames[method].add(field.get_name())
#DEPRECATED|                        map_method_fieldnames[method].add(key_field.get_name())
#DEPRECATED|
#DEPRECATED|                        # demux annotation
#DEPRECATED|                        method_name = method.get_name()
#DEPRECATED|                        if method_name != table_name :
#DEPRECATED|                            if method_name not in map_tablename_methods.keys():
#DEPRECATED|                                map_tablename_methods[method_name] = set()
#DEPRECATED|                            map_tablename_methods[method_name].add(method)
#DEPRECATED|
#DEPRECATED|                        platforms.add(method.get_platform())
#DEPRECATED|
#DEPRECATED|            table = Table(platforms, table_name, fields, keys)
#DEPRECATED|
#DEPRECATED|            # XXX Hardcoded capabilities in 3nf tables
#DEPRECATED|            table.capabilities.retrieve   = True
#DEPRECATED|            table.capabilities.join       = True
#DEPRECATED|            table.capabilities.selection  = True
#DEPRECATED|            table.capabilities.projection = True
#DEPRECATED|
#DEPRECATED|            # inject field and key annotation in the Table object
#DEPRECATED|            table.map_method_keys   = map_method_keys
#DEPRECATED|            table.map_method_fieldnames = map_method_fieldnames
#DEPRECATED|            tables_3nf.append(table)
#DEPRECATED|            all_tables.append(table)
#DEPRECATED|            Log.debug("TABLE 3nf (i):", table, keys)
#DEPRECATED|            #print "     method fields", map_method_fieldnames
#DEPRECATED|
#DEPRECATED|            num_platforms += 1
#DEPRECATED|            all_platforms |= platforms
#DEPRECATED|
#DEPRECATED|            if not common_fields:
#DEPRECATED|                common_fields = fields
#DEPRECATED|            else:
#DEPRECATED|                common_fields = merge_fields(fields, common_fields)
#DEPRECATED|
#DEPRECATED|            #if not common_keys:
#DEPRECATED|            #    common_keys = keys
#DEPRECATED|            #else:
#DEPRECATED|            #    #common_keys &= keys
#DEPRECATED|            #    common_keys = merge_keys(keys, common_keys)
#DEPRECATED|
#DEPRECATED|            # Collect possible keys, we will restrict this set once
#DEPRECATED|            # common_fields will be computed.
#DEPRECATED|            all_keys |= keys
#DEPRECATED|
#DEPRECATED|        # Check whether we will add a parent table. If so compute the
#DEPRECATED|        # corresponding Keys based on all_keys and common_fields.
#DEPRECATED|        common_keys = None
#DEPRECATED|        if num_platforms > 1 and len(common_fields) > 0:
#DEPRECATED|            # Retrict common_keys according to common_fields
#DEPRECATED|            common_field_names = FieldNames([field.get_name() for field in common_fields])
#DEPRECATED|            common_keys = Keys([key for key in all_keys if key.get_field_names() <= common_field_names])
#DEPRECATED|
#DEPRECATED|        # Need to add a parent table if more than two sets of platforms
#DEPRECATED|        # XXX SOmetimes this parent table already exists and we are just
#DEPRECATED|        # duplicating it... (not solved at the moment) XXX
#DEPRECATED|        if common_keys:
#DEPRECATED|
#DEPRECATED|            # Capabilities will be set later since they must be set for all the Tables.
#DEPRECATED|            table = Table(all_platforms, table_name, common_fields, common_keys)
#DEPRECATED|
#DEPRECATED|            # Migrate common fields from children to parents, except keys
#DEPRECATED|            ##map_common_method_keys   = dict()
#DEPRECATED|            map_common_method_fieldnames = dict()
#DEPRECATED|
#DEPRECATED|            for field in common_fields:
#DEPRECATED|                methods = set()
#DEPRECATED|                for child_table in all_tables:
#DEPRECATED|                    # Objective = remove the field from child table
#DEPRECATED|                    # Several methods can have it
#DEPRECATED|                    for _method, _fields in child_table.map_method_fieldnames.items():
#DEPRECATED|                        if field.get_name() in _fields:
#DEPRECATED|                            methods.add(_method)
#DEPRECATED|                            if not common_keys.has_field(field):
#DEPRECATED|                                _fields.remove(field.get_name())
#DEPRECATED|
#DEPRECATED|                if not common_keys.has_field(field):
#DEPRECATED|                    del child_table.fields[field.get_name()]
#DEPRECATED|                # Add the field with all methods to parent table
#DEPRECATED|                for method in methods:
#DEPRECATED|                    if not method in map_common_method_fieldnames: map_common_method_fieldnames[method] = set()
#DEPRECATED|                    map_common_method_fieldnames[method].add(field.get_name())
#DEPRECATED|
#DEPRECATED|            # Inject field and key annotation in the Table object
#DEPRECATED|            table.map_method_keys  = dict() #map_common_method_keys
#DEPRECATED|            table.map_method_fieldnames = map_common_method_fieldnames
#DEPRECATED|
#DEPRECATED|            # XXX Hardcoded capabilities in 3nf tables
#DEPRECATED|            table.capabilities.retrieve   = True
#DEPRECATED|            table.capabilities.join       = True
#DEPRECATED|            table.capabilities.selection  = True
#DEPRECATED|            table.capabilities.projection = True
#DEPRECATED|
#DEPRECATED|            tables_3nf.append(table)
#DEPRECATED|            Log.debug("TABLE 3nf (ii):", table, table.get_keys())
#DEPRECATED|            #print "     method fields", map_common_method_fieldnames
#DEPRECATED|
#DEPRECATED|
#DEPRECATED|        # XXX we already know about the links between those two platforms
#DEPRECATED|        # but we can find them easily (cf dbgraph)
#DEPRECATED|
#DEPRECATED|    # inject demux annotation
#DEPRECATED|    for table_3nf in tables_3nf:
#DEPRECATED|        if table_3nf.get_name() in map_tablename_methods.keys():
#DEPRECATED|            table_3nf.methods_demux = map_tablename_methods[table_3nf.get_name()]
#DEPRECATED|        else:
#DEPRECATED|            table_3nf.methods_demux = set()
#DEPRECATED|
#DEPRECATED|    # Compute Capabilities corresponding to the union of the
#DEPRECATED|    # Capabilities of each child Table.
#DEPRECATED|    for table_3nf in tables_3nf:
#DEPRECATED|        capabilities = Capabilities()
#DEPRECATED|        for platform_name in all_platforms:
#DEPRECATED|            announces = metadata[platform_name]
#DEPRECATED|            for announce in announces:
#DEPRECATED|                if announce.get_table().get_name() == table_name:
#DEPRECATED|                    break
#DEPRECATED|            capabilities = merge_capabilities(capabilities, announce.get_table().get_capabilities())
#DEPRECATED|        table_3nf.set_capability(capabilities)
#DEPRECATED|
#DEPRECATED|#    # 6) Inject capabilities
#DEPRECATED|#    # TODO: capabilities are now in tables, shall they be present in tables_3nf
#DEPRECATED|#    # instead of relying on map_method_capabilities ?
#DEPRECATED|#    for table_3nf in tables_3nf:
#DEPRECATED|#        for announces in metadata.values():
#DEPRECATED|#            for announce in announces:
#DEPRECATED|#                if announce.get_table().get_name() == table_3nf.get_name():
#DEPRECATED|#                    capabilities = table_3nf.get_capabilities()
#DEPRECATED|#                    if capabilities.is_empty():
#DEPRECATED|#                        table_3nf.set_capability(announce.get_table().get_capabilities())
#DEPRECATED|#                    elif capabilities != announce.get_table().get_capabilities():
#DEPRECATED|#                        Log.warning("Conflicting capabilities for tables %r and %r" % (table_3nf, announce.get_table()))
#DEPRECATED|
#DEPRECATED|    # Adding local tables
#DEPRECATED|    tables_3nf.extend(local_tables)
#DEPRECATED|
#DEPRECATED|    # 7) Building DBgraph
#DEPRECATED|    graph_3nf = DBGraph(frozenset(tables_3nf), map_method_capabilities)
#DEPRECATED|
#DEPRECATED|    return graph_3nf
#DEPRECATED|
