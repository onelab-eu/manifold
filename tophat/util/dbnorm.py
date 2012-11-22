from tophat.core.table import Table
class DBNorm:
    """
    Database schema normalization support
    http://elm.eeng.dcu.ie/~ee221/EE221-DB-7.pdf
    """

    def __init__(self, tables = None):
        """
        Initializes the set of tables
        """
        if tables == None:
            self.tables = []
        else:
            self.tables = tables
        self.tables_3nf = self.to_3nf()

    def attribute_closure(self, attributes, fd_set):
        """
        Compute the closure of a set of attributes under the set of functional dependencies fd_set
        """
        if not isinstance(attributes, (set, frozenset)):
            closure = set([attributes])
        else:
            closure = set(attributes)
        while True:
            old_closure = closure.copy()
            for y, z in fd_set: # Y -> Z
                if y in closure:
                    closure.add(z)
            if old_closure == closure:
                break
        return closure
    
    def fd_minimal_cover(self, fd_set):
        # replace each FD X -> (A1, A2, ..., An) by n FD X->A1, X->A2, ..., X->An
        min_cover = set([(y, attr) for y, z in fd_set for attr in z if y != attr])
        for x, a in min_cover.copy():
            reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
            x_plus = self.attribute_closure(x, reduced_min_cover)
            if a in x_plus:
                min_cover = reduced_min_cover
        for x, a in min_cover:
            if isinstance(x, frozenset):
                for b in x:
                    # Compute (X-B)+ with respect to (G-(X->A)) U ((X-B)->A) = S
                    x_minus_b = frozenset([i for i in x if i != b])
                    s = set([fd for fd in min_cover if fd != (x,a)])
                    s.add((x_minus_b, a))
                    x_minus_b_plus = self.attribute_closure(x_minus_b, s) 
                    if b in x_minus_b_plus:
                        reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
                        min_cover = reduced_min_cover
                        min_cover.add( (x_minus_b, a) )
        return min_cover

    def to_3nf(self):
        return self.to_3nf_old()

    #---------------------------------------------------------------------------
    def to_3nf_old(self):
        #fd_set = set([(key, table.get_field_names()) for table in self.tables for key in table.keys])
        fd_set = set([(key, table.fields) for table in self.tables for key in table.get_fields_from_keys()])

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        determinants = {}
        for x, a in fd_set:
            if len(list(x)) == 1:
                x = list(x)[0]
            if not x in determinants:
                determinants[x] = set([])
            determinants[x].add(a)
        
        # ... create relation R = (X, A1, A2, ..., An)
        # determinants: key: A MetadataClass key (array of strings), data: MetadataField
        relations = []
        for x, y in determinants.items():
            # Search source tables related to the corresponding key x and values
            sources = [t for t in self.tables if t.is_key(x)]
            if not sources:
                raise Exception("No source table found with key %s" % x)

            # Several platforms provide the requested table, we choose an arbitrary one
            p = [s.platform for s in sources]
            if len(p) == 1:
                p = p[0]

            n = list(sources)[0].name
            fields = list(y)
            if isinstance(x, (frozenset, tuple)):
                fields.extend(list(x))
            else:
                fields.append(x)
            k = [xi.field_name for xi in x] if isinstance(x, (frozenset, tuple)) else x.field_name
            t = Table(p, n, fields, [k])
            relations.append(t)
        return relations

    #---------------------------------------------------------------------------

    def to_3nf_new(self):
        # Build the set of functional dependencies
        fd_set = set([(key, table.fields) for table in self.tables for key in table.get_fields_from_keys()])

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        # = map each 'cur_key' vertex to its successors ('fields')
        determinants = {}
        for cur_key, cur_fields in fd_set:
            if not cur_key in determinants:
                determinants[cur_key] = set([])
            determinants[cur_key].add(cur_fields)

        # ... create relation R = (X, A1, A2, ..., An)
        # determinants: key: An array of MetadataField (key of the table), data: MetadataField
        relations = []
        for cur_key, cur_fields in determinants.items():
            # Platform list and names for the corresponding key cur_key and values
            src_tables = [table for table in self.tables if cur_key in table.get_fields_from_keys()]
            src_plateforms = [src_table.platform for src_table in src_tables]
            if len(src_plateforms) == 1:
                src_plateforms = src_plateforms[0]
            src_tablename = list(src_tables)[0].name

            # Note, we do not manage multiple keys here...d
            fields = list(cur_fields)
            if isinstance(cur_key, (frozenset, tuple)):
                fields.extend(list(cur_key))
            else:
                fields.append(cur_key)
            t = Table(src_plateforms, src_tablename, fields, [cur_key])
            
            relations.append(t)
        return relations
