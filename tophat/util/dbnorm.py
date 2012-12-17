from tophat.core.table import Table
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
        def in_closure(y, closure):
            if isinstance(y, (set, frozenset, tuple, list)):
                y_in_closure = True
                for y_elt in y:
                    y_in_closure &= y_elt in closure
                return y_in_closure
            else:
                return y in closure

        # Transform attributes into a set
        if not isinstance(attributes, (set, frozenset, tuple, list)):
            closure = set([attributes])
        else:
            closure = set(attributes)

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

        for key, a in min_cover.copy():
            reduced_min_cover = set([fd for fd in min_cover if fd != (key,a)])
            x_plus = self.attribute_closure(key, reduced_min_cover)
            if a in x_plus:
                min_cover = reduced_min_cover

        for key, a in min_cover:
            if isinstance(key, frozenset):
                for b in key:
                    # Compute (X-B)+ with respect to (G-(X->A)) U ((X-B)->A) = S
                    x_minus_b = frozenset([i for i in key if i != b])
                    s = set([fd for fd in min_cover if fd != (key,a)])
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
                triplet = (key, platforms, method)
                if triplet not in fd_set0.keys():
                    fd_set0[triplet] = set()
                fd_set0[triplet].add(field)

        fd_set = set()
        for triplet, fields in fd_set0.items():
            fd_set.add((triplet, frozenset(fields)))

        return fd_set

    #---------------------------------------------------------------------------
    def to_3nf_new(self):
        fd_set = self.make_fd_set()

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        determinants = {}
        for triple, fields in fd_set:
            if len(list(triple)) == 1:
                triple = list(triple)[0]
            if not triple in determinants:
                determinants[triple] = set([])
            determinants[triple].add(fields)
        
        # ... create relation R = (X, A1, A2, ..., An)
        # determinants: key: A MetadataClass key (array of strings), data: MetadataField
        relations = []
        for triple, fields in determinants.items():
            # Search source tables related to the corresponding key key and values
            key, platforms, method = triple
            sources = [table for table in self.tables if table.is_key(key)]
            if not sources:
                raise Exception("No source table found with key %s" % key)

            fields = list(fields)
            if isinstance(key, (frozenset, tuple)):
                fields.extend(list(key))
            else:
                fields.append(key)
            k = [xi.field_name for xi in key] if isinstance(key, (frozenset, tuple)) else key.field_name
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
            if isinstance(key, (frozenset, tuple)):
                fields.extend(list(key))
            else:
                fields.append(key)
            k = [xi.field_name for xi in key] if isinstance(key, (frozenset, tuple)) else key.field_name
            t = Table(partitions, n, fields, [k])
            relations.append(t)
        return relations


