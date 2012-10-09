from tophat.core.table import Table
class DBNorm:
    """
    Database schema normalization support
    """

    def __init__(self, tables = []):
        """
        Initializes the set of tables
        """
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
        # Build the set of functional dependencies
        fd_set = set([(key, g.fields) for g in self.tables for key in g.keys])

        # Find a minimal cover
        fd_set = self.fd_minimal_cover(fd_set)
        
        # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
        # containing all FDs in G with the same determinant X ...
        determinants = {}
        for x, a in fd_set:
            if not x in determinants:
                determinants[x] = set([])
            determinants[x].add(a)

        # ... create relaton R = (X, A1, A2, ..., An)
        relations = []
        for x, y in determinants.items():
            # Platform list and names for the corresponding key x and values
            sources = [t for t in self.tables if x in t.keys]
            p = [s.platform for s in sources]
            if len(p) == 1: p = p[0]
            n = list(sources)[0].name
            # Note, we do not manage multiple keys here...d
            fields = list(y)
            if isinstance(x, frozenset):
                fields.extend(list(x))
            else:
                fields.append(x)
            t = Table(p, n, fields, [x])
            relations.append(t)
        return relations
