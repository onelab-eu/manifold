#!/usr/bin/env python
# -*- coding:utf-8 -*-

# inclusion lattice... like a binary tree but for partial order

SEARCH_EQUAL            = 0
SEARCH_EQUAL_OR_GREATER = 1

class LatticeElement(object):
    def __init__(self, element=None, data = None):
        self.element  = element
        self.data     = data
        self.parents  = set()
        self.children = set()


    def __eq__(self, other):
        return self.element == other.element

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return self.element <= other.element

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return self.element >= other.element

    def __gt__(self, other):
        return self.element > other.element


    def __str__(self):
        return str(self.element)

    def __repr__(self):
        return "<LatticeElement %r>" % self.element

class Top(LatticeElement):
    def __eq__(self, other):
        return isinstance(other.element, Top)

    def __le__(self, other):
        return False

    def __ge__(self, other):
        return True

    def __str__(self):
        return 'TOP'

    def __repr__(self):
        return '<LatticeElement TOP>'

class Bottom(LatticeElement):
    def __eq__(self, other):
        return isinstance(other.element, Bottom)

    def __le__(self, other):
        return True

    def __ge__(self, other):
        return False

    def __str__(self):
        return 'BOTTOM'

    def __repr__(self):
        return '<LatticeElement BOTTOM>'

class Lattice(object):
    def __init__(self, enforce_type=None):
        self.top    = Top()
        self.bottom = Bottom()

        self.top.children.add(self.bottom)
        self.bottom.parents.add(self.top)

    def search(self, element):
        """
        returns min_max_set and max_min_set
        if they are both equal, that means the element has been found
        """
        # We build a LatticeElement for the comparison (top, bottom)
        le = LatticeElement(element)

        # We use a top-down approach and aim at building two sets:
        #  - the minimal-majorant set (min_max_set)
        #  - the maximal-minorant set (max_min_set)
        # All elements in the sets are not comparable, and represent minimal (resp. maximal) values in the lattice
        # Note: the same approach could be done bottom-up.

        min_max_set = set([self.top])
        max_min_set = set()

        # We stop when the min_max_set stabilizes
        min_max_set_old = set()

        while min_max_set != min_max_set_old:
            min_max_set_old = min_max_set
            min_max_set = set()
            # x is not a min_max if it exists y < x in the lattice such as y > e
            # y < x means y is a child of x
            for x in min_max_set_old:
                discard_x = False
                for y in x.children:
                    if y == le:
                        # Found element
                        return (set([y]), set([y]))
                    elif y <= le:
                        # y is a candidate for the max_min_set
                        max_min_set_old = max_min_set
                        max_min_set = set()
                        discard_y = False
                        # y is not a max min if it exists z in max_min_set such as y < z
                        for z in max_min_set_old:
                            if not z <= y:
                                if y <= z:
                                    discard_y = True
                                max_min_set.add(z)
                            #else:
                            #    # z should not be in max_min_set
                            #    pass
                        if not discard_y:
                            max_min_set.add(y)
                    elif le <= y:
                        # le <= y <= x
                        discard_x = True
                        min_max_set.add(y)
                    else:
                        min_max_set.add(y)
                if not discard_x:
                    min_max_set.add(x)
                            
        if not max_min_set:
            max_min_set = set([self.bottom])
        return (min_max_set, max_min_set)

    def _get(self, element, search_type = SEARCH_EQUAL):
        """
        \return set of lattice_element
        """
        min_max_set, max_min_set = self.search(element)
        if min_max_set == max_min_set:
            assert len(min_max_set) == 1, "min_max_set == max_min_set and length != 1"
            return min_max_set
        else:
            if search_type == SEARCH_EQUAL:
                return None
            if self.top in min_max_set:
                return None
            return min_max_set

    def _get_best(self, element):
        lattice_elements = self._get(element, SEARCH_EQUAL_OR_GREATER)
        if not lattice_elements:
            return None
        if len(lattice_elements) == 1:
            return iter(lattice_elements).next()
        if len(lattice_elements) > 1:
            # XXX Need to elect the best, eg. based on timestamps
            return iter(lattice_elements).next()

    def get_best(self, element):
        le = self._get_best(element)
        if not le:
            return None
        return (le.element, le.data)

    def get_data(self, element):
        le = self._get(element, SEARCH_EQUAL)
        if not le:
            return None
        le = iter(le).next()
        return le.data

    def _update(self, lattice_element, data, recursive, update_data_callback):
        lattice_element.data = data

        if not recursive:
            return

        if update_data_callback:
            to_update  = set()
            to_update |= self._get_greater(lattice_element)
            to_update |= self._get_lesser(lattice_element)

            for le in to_update: 
                try:
                    new_data = update_data_callback(updated_element, updated_data, element, data)
                    le.data = new_data
                except:
                    self._delete(le)
        else:
            self._delete(le)

    def add(self, element = None, data = None, replace = False, update = False, update_data_callback = None):
        min_max_set, max_min_set = self.search(element)
        if min_max_set == max_min_set:
            assert len(min_max_set) == 1, "min_max_set == max_min_set and length != 1"
            # Element found in lattice
            if not replace:
                raise Exception, "Element already in lattice"
            le = iter(min_max_set).next()

            self._update(le, data, update, update_data_callback)
            return

        le = LatticeElement(element, data)
        le.parents  = min_max_set

        for x in min_max_set:
            x.children -= max_min_set
            x.children.add(le)

        le.children = max_min_set

        for x in max_min_set:
            x.parents -= min_max_set
            x.parents.add(le)

    # Add and update are also similar
    def update(self, element = None, data = None, recursive = False, update_data_callback=None):
        min_max_set, max_min_set = self.search(element)
        if min_max_set != max_min_set:
            raise Exception, "Element not found in lattice"
        le = iter(min_max_set).next()
        return self._update(le, data, recursive, update_data_callback)

    def _get_greater(self, lattice_element):
        old_greater = set()
        greater = lattice_element.parents
        
        while greater != old_greater:
            old_greater = greater
            greater     = set()

            for x in old_greater:
                if x.parents != self.top:
                    greater |= x.parents
                else:
                    greater.add(x)

        return greater

    def _get_lesser(self, lattice_element):
        old_lesser  = set()
        lesser  = lattice_element.children

        while lesser != old_lesser:
            old_lesser = lesser
            lesser     = set()

            for x in old_lesser:
                if x.children != self.bottom:
                    lesser |= x.children
                else:
                    lesser.add(x)

        return lesser

    def _delete(self, lattice_element, recursive = False):
        # Does nothing if the element has already been deleted from the
        # lattice, ie. both parents and children are empty
        for parent in lattice_element.parents:
            parent.children |= lattice_element.children
            parent.children -= lattice_element
        for child in lattice_element.children:
            child.parents   |= lattice_element.parents
            child.parents   -= lattice_element

        if recursive:
            greater = self._get_greater(lattice_element)
            for le in greater:
                self._delete(le)
            lesser  = self._get_lesser(lattice_element)
            for le in lesser:
                self._delete(le)
            
        # Marks the LatticeElement as deleted (and free references counters)
        # We do not store None so that functions like _delete can continue to
        # work if called on already deleted elements
        lattice_element.children = []
        lattice_element.parents  = []

    def delete(self, element, recursive = False):
        min_max_set, max_min_set = self.search(element)
        if min_max_set != max_min_set:
            raise Exception, "Element not found in lattice"
        le = iter(min_max_set).next()

        return self._delete(le, recursive)

    def invalidate(self, element, recursive = False):
        # Unlike delete, invalidate can be called even if the element is not found in the lattice
        min_max_set, max_min_set = self.search(element)
        if min_max_set == max_min_set:
            le = iter(min_max_set).next()
            self._delete(le, recursive)
            return

        if not recursively:
            return

        # The query is not found, we have to delete recursively
        greater = set()
        for le in min_max_set:
            greater |= self._get_greater(le)
        for le in greater:
            self._delete(le)

        lesser = set()
        for le in max_min_set:
            lesser |= self._get_lesser(le)
        for le in lesser:
            self._delete(le)

    def dump(self):
        out = [
            "digraph G {",
            "  splines=\"line\"",
#            "  rankdir=BT",
            "  \"TOP\" [shape=box]",
            "  \"BOTTOM\" [shape=box]"
        ]

        seen = set()
        stack = [self.top]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)

            for child in current.children:
                out.append("  \"%s\" -> \"%s\"" % (current, child))
                stack.append(child)
        out.append("}")
        return "\n".join(out)

################################################################################

if __name__ == '__main__':
    
    from manifold.core.query import Query

    q1 = Query.get('table').filter_by('prop', '==', 1)
    q2 = Query.get('table').select('test')
    q3 = Query.get('table').filter_by('prop', '==', 1).select('test')
    q4 = Query.get('table')

    l = Lattice()

    for i, q in enumerate([q1, q2, q3, q4]):
        #print '#'*80
        #print "# q%d # %s" % (i+1, q)
        #print '#'*80
        l.add(q)

    print l.dump()
