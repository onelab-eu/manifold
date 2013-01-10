from tophat.core.table import Table
from tophat.core.key   import Key 
from types             import StringTypes
from tophat.util.type  import returns, accepts

class Determinant:
    def __init__(self, key, platforms, method):
        if not isinstance(key, Key):
            raise TypeError("Invalid key %r (type = %r)" % (key, type(key)))
        if not isinstance(platforms, frozenset):
            raise TypeError("Invalid platforms %r (type = %r)" % (platforms, type(platforms)))
        if not isinstance(method, StringTypes):
            raise TypeError("Invalid method %r (type = %r)" % (method, type(method))) 
        self.key = key
        self.platforms = platforms
        self.method = method

    @returns(Key)
    def get_key(self):
        return self.key

    @returns(frozenset)
    def get_platforms(self):
        return self.platforms

    @returns(str)
    def get_method(self):
        return self.method

    def __hash__(self):
        return hash((
            self.get_key(),
            self.get_platforms(),
            self.get_method()
        ))

    @returns(bool)
    def __eq__(self, x):
        if not isinstance(x, Determinant):
            raise TypeError("Invalid paramater %r of type %r" % (x, type(x)))
        return self.get_key() == x.get_key() and self.get_platforms() == x.get_platforms() and self.get_method() == x.get_method()

    @returns(str)
    def __str__(self):
        return (
            self.get_key(),
            self.get_platforms(),
            self.get_method()
        ).__str__()

    @returns(str)
    def __repr__(self):
        return self.__str__()


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

    def attribute_closure(self, attributes, fd_set):
        """
        \brief Compute the closure of a set of attributes under the set of functional dependencies fd_set
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p7
        \param attributes The attributes (~ source vertices)
        \param fd_set The functionnal dependancies (~ arcs)
            A fd is a tuple made of the key (e.g a tuple of fields) and a field 
        \return The corresponding closure (~ reachable vertices)
        """
        def in_closure(determinant, closure):
            if not isinstance(determinant, Determinant):
                raise TypeError("Invalid type of determinant (%r)" % type(determinant))
            key = determinant.get_key()
            if key.is_composite():
                key_in_closure = True
                for key_elt in key:
                    key_in_closure &= key_elt in closure
                return key_in_closure
            else:
                return key in closure
            return False

        # Transform attributes into a set
        if not isinstance(attributes, (set, frozenset, tuple, list)):
            closure = set([attributes])
        else:
            closure = set(attributes)

#        print ">> INIT: attribute_closure:"
#        print ">> fd_set = %r" % fd_set
#        print ">> attributes = %r" % closure 

        # Compute closure
        while True:
            old_closure = closure.copy()
            for y, z in fd_set: # y -> z
                if in_closure(y, closure):
                    closure.add(z)
            if old_closure == closure:
                break
        return closure
    
    def fd_minimal_cover(self, fd_set):
        """
        \brief Compute the functionnal dependancy minimal cover
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p10
        \param fd_set The functionnal dependancies
        \return The minimal cover
        """
        # replace each FD X -> (A1, A2, ..., An) by n FD X->A1, X->A2, ..., X->An
        min_cover = set([(y, attr) for y, z in fd_set for attr in z if y != attr])

        print "-" * 100
        print "1) min_cover = ", min_cover
        print "-" * 100

        for determinant, a in min_cover.copy():
            reduced_min_cover = set([fd for fd in min_cover if fd != (determinant, a)])
            x_plus = self.attribute_closure(determinant, reduced_min_cover)
            if a in x_plus:
                min_cover = reduced_min_cover

        print "-" * 100
        print "2) min_cover = ", min_cover
        print "-" * 100

        for determinant, a in min_cover:
            key = determinant.get_key() 
            if key.is_composite():
                for b in key:
                    # Compute (X-B)+ with respect to (G-(X->A)) U ((X-B)->A) = S
                    x_minus_b = frozenset([i for i in key if i != b])
                    print "min_cover = %r " % min_cover
                    print "key = %r" % key
                    print "a = %r" % a
                    s = set([fd for fd in min_cover if fd != (key, a)])
                    s.add((x_minus_b, a))
                    x_minus_b_plus = self.attribute_closure(x_minus_b, s) 
                    if b in x_minus_b_plus:
                        reduced_min_cover = set([fd for fd in min_cover if fd != (key,a)])
                        min_cover = reduced_min_cover
                        min_cover.add( (x_minus_b, a) )

        return min_cover

    def to_3nf(self):
        """
        \brief Compute a 3nf schema according to self.tables
        \sa http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf p14
        \return The corresponding 3nf schema
        """
        return self.to_3nf_new()

    def make_fd_set(self):
        rules = {}
        for table in self.tables:
            method = table.name
            platforms = table.get_platforms()
            for key in table.get_fields_from_keys():
                for field in table.get_fields():
                    if (key, method) not in rules.keys():
                        rules[(key, method)] = {}
                    if field not in rules[(key, method)]:
                        rules[(key, method)][field] = set()
                    rules[(key, method)][field] |= platforms

        fd_set0 = {}
        for rule, map_field_platforms in rules.items():
            (key, method) = rule
            for field, platforms in map_field_platforms.items():
                platforms = frozenset(platforms)
                determinant = Determinant(key, platforms, method)
                if determinant not in fd_set0.keys():
                    print "fd_set0 = ", fd_set0
                    print "determinant = ", determinant
                    fd_set0[determinant] = set()
                fd_set0[determinant].add(field)

        fd_set = set()
        for determinant, fields in fd_set0.items():
            fd_set.add((determinant, frozenset(fields)))

        return fd_set

    #---------------------------------------------------------------------------
    def to_3nf_new(self):
        fd_set = self.make_fd_set()

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)

        print "fd_set = %r" % fd_set
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        determinants = {}
        for determinant, fields in fd_set:
            if not determinant in determinants:
                determinants[determinant] = set([])
            determinants[determinant].add(fields)
        
        # ... create relation R = (X, A1, A2, ..., An)
        # determinants: key: A MetadataClass key (array of strings), data: MetadataField
        relations = []
        for determinant, fields in determinants.items():
            # Search source tables related to the corresponding key key and values
            key = determinant.get_key()
            platforms = determinant.get_key()
            method = determinant.get_method()
            sources = [table for table in self.tables if table.is_key(key)]
            if not sources:
                raise Exception("No source table found with key %s" % key)

            fields = list(fields)
            if key.is_composite():
                fields.extend(list(key))
            else:
                fields.append(key)
            k = [xi.get_name() for xi in key] if key.is_composite() else key.get_name()
            t = Table(platforms, method, fields, [k])
            relations.append(t)
        return relations


    #---------------------------------------------------------------------------
    def to_3nf_bak(self):
        #fd_set = set([(key, table.get_field_names()) for table in self.tables for key in table.keys])
        fd_set = set([(key, table.fields) for table in self.tables for key in table.get_fields_from_keys()])

        for x in list(fd_set):
            print "FD_SET", x

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        determinants = {}
        for key, a in fd_set:
            if len(list(key)) == 1:
                key = list(key)[0]
            if not key in determinants:
                determinants[key] = set([])
            determinants[key].add(a)
        
        # ... create relation R = (X, A1, A2, ..., An)
        # determinants: key: A MetadataClass key (array of strings), data: MetadataField
        relations = []
        for key, y in determinants.items():
            # Search source tables related to the corresponding key key and values
            sources = [t for t in self.tables if t.is_key(key)]
            if not sources:
                raise Exception("No source table found with key %s" % key)

            partitions = dict()
            for source in sources:
                for plaforms, clause in source.get_partitions():
                    if not plaforms in partitions: 
                        partitions[plaforms] = clause 
                    else:
                        # Several partitions provide the requested table, we've
                        # already choose an arbitrary one and we ignore this one
                        print "TODO union clause"

            n = list(sources)[0].name
            fields = list(y)
            if key.is_composite():
                fields.extend(list(key))
            else:
                fields.append(key)
            k = [xi.get_name() for xi in key] if key.is_composite() else key.get_name()
            t = Table(partitions, n, fields, [k])
            relations.append(t)
        return relations


