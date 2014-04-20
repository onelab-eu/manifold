# -*- coding: utf-8 -*-

from types                          import StringTypes
from manifold.util.type             import accepts, returns

# The distinction between parent and children fields is based on the
# Fields.FIELD_SEPARATOR character.
#
# See manifold.operators.subquery
FIELD_SEPARATOR = '.'
DEFAULT_IS_STAR = False

class Fields(list):
    """
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        star = kwargs.pop('star', DEFAULT_IS_STAR)
        list.__init__(self, *args, **kwargs)
        self._star = False if list(self) else star

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The %r representation of this Fields instance.
        """
        if self.is_star():
            return "<Fields *>"
        else:
            return "<Fields %r>" % [x for x in self]

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The %s representation of this Fields instance.
        """
        if self.is_star():
            return "<*>"
        else:
            return "<%r>" % [x for x in self]

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    @returns(bool)
    def is_star(self):
        """
        Returns:
            True iif this Fields instance correspond to "any Field" i.e. "*".
            Example : SELECT * FROM foo
        """
        return self._star

    def set_star(self):
        """
        Update this Fields instance to make it corresponds to "*"
        """
        self._star = True
        self.clear()

    def unset_star(self, fields = None):
        """
        Update this Fields instance to make it corresponds to a set of Fields
        Args:
            fields: A Fields instance or a set of Field instances.
        """
        self._star = False
        if fields:
            self |= fields

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif Fields instance designates contains least one Field.
        """
        return not self.is_star() and not self

    #@returns(Fields)
    def copy(self):
        """
        Returns:
            A copy of this Fields instance.
        """
        return Fields(self[:])

    #---------------------------------------------------------------------------
    # Iterators
    #---------------------------------------------------------------------------

    def iter_field_subfield(self):
        for f in self:
            field, _, subfield = f.partition(FIELD_SEPARATOR)
            yield (field, subfield)

    #---------------------------------------------------------------------------
    # Overloaded set internal functions
    #---------------------------------------------------------------------------

    #@returns(Fields)
    def __or__(self, fields):
        """
        Compute the union of two Fields instances.
        Args:
            fields: a set of Field instances or a Fields instance.
        Returns:
            The union of the both Fields instance.
        """
        if self.is_star() or fields.is_star():
            return Fields(star = True)
        else:
            l = self[:]
            l.extend([x for x in fields if x not in l])
            return Fields(l)

    #@returns(Fields)
    def __ior__(self, fields):
        """
        Compute the union of two Fields instances.
        Args:
            fields: a set of Field instances or a Fields instance.
        Returns:
            The updated Fields instance.
        """
        if self.is_star() or fields.is_star():
            self.set_star()
            return self
        else:
            self.extend([x for x in fields if x not in self])
            return self

    #@returns(Fields)
    def __and__(self, fields):
        """
        Compute the intersection of two Fields instances.
        Args:
            fields: a set of Field instances or a Fields instance.
        Returns:
            The intersection of the both Fields instances.
        """
        if self.is_star():
            return fields.copy()
        elif fields.is_star():
            return self.copy()
        else:
            return Fields([x for x in self if x in fields])

    #@returns(Fields)
    def __iand__(self, fields):
        """
        Compute the intersection of two Fields instances.
        Args:
            fields: a set of Field instances or a Fields instance.
        Returns:
            The updated Fields instance.
        """
        if self.is_star():
            self.unset_star(fields)
        elif fields.is_star():
            pass
        else:
            self[:] = [x for x in self if x in fields]

    @returns(bool)
    def __nonzero__(self):
        return self.is_star() or bool(list(self))

    # Python>=3
    __bool__ = __nonzero__

    __add__ = __or__

    #@returns(Fields)
    def __sub__(self, fields):
        if fields.is_star():
            return Fields(star = False)
        else:
            if self.is_star():
                # * - x,y,z = ???
                return Fields(star = True) # XXX NotImplemented
            else:
                return Fields([x for x in self if x not in fields])

    def __isub__(self, fields):
        print "isub"
        raise NotImplemented

    def __iadd__(self, fields):
        print "iadd"
        raise NotImplemented

    #---------------------------------------------------------------------------
    # Overloaded set comparison functions
    #---------------------------------------------------------------------------

    @returns(bool)
    def __eq__(self, other):
        """
        Test whether this Fields instance corresponds to another one.
        Args:
            other: The Fields instance compared to self.
        Returns:
            True if the both Fields instance matches.
        """
        return self.is_star() and other.is_star() or list.__eq__(self, other)

    @returns(bool)
    def __le__(self, other):
        """
        Test whether this Fields instance in included in
        (or equal to) another one.
        Args:
            other: The Fields instance compared to self or
        Returns:
            True if the both Fields instance matches.
        """
        assert isinstance(other, Fields),\
            "Invalid other = %s (%s)" % (other, type(other))

        return (self.is_star() and other.is_star())\
            or (not self.is_star() and other.is_star())\
            or (list.__le__(self, other))

    # Defined with respect of previous functions

    @returns(bool)
    def __ne__(self, other):
        """
        Test whether this Fields instance differs to another one.
        Args:
            other: The Fields instance compared to self.
        Returns:
            True if the both Fields instance differs.
        """
        return not self == other

    @returns(bool)
    def __lt__(self, other):
        """
        Test whether this Fields instance in strictly included in
        another one.
        Args:
            other: The Fields instance compared to self.
        Returns:
            True if self is strictly included in other.
        """
        return self <= other and self != other

    @returns(bool)
    def __ge__(self, other):
        return other.__le__(self)

    @returns(bool)
    def __gt__(self, other):
        return other.__lt__(self)

    #---------------------------------------------------------------------------
    # Overloaded set functions
    #---------------------------------------------------------------------------

    def add(self, field_name):
        # DEPRECATED
        self.append(field_name)

    def append(self, field_name):
        if not isinstance(field_name, StringTypes):
            raise TypeError("Invalid field_name name %s (string expected, got %s)" % (field_name, type(field_name)))

        if not self.is_star():
            list.append(self, field_name)

    def clear(self):
        self._star = False
        del self[:]

    def rename(self, aliases):
        s = self.copy()
        for element in s:
            if element in aliases:
                s.remove(element)
                s.add(aliases[element])
        self.clear()
        self |= s
        return self

    @staticmethod
    @returns(StringTypes)
    def join(field, subfield):
        return "%s%s%s" % (field, FIELD_SEPARATOR, subfield)

    @staticmethod
    @returns(tuple)
    def after_path(field, path, allow_shortcuts = True):
        """
        Returns the part of the field after path

        Args:
            path (list):
            allow_shortcuts (bool): Default to True.
        """
        if not path:
            return (field, None)
        last = None
        field_parts = field.split(FIELD_SEPARATOR)
        for path_element in path[1:]:
            if path_element == field_parts[0]:
                field_parts.pop(0)
                last = None
            else:
                last = path_element
        return (FIELD_SEPARATOR.join(field_parts), last)

    @returns(tuple)
    def split_subfields(self, include_parent = True, current_path = None, allow_shortcuts = True):
        """
        Returns a tuple of Fields + dictionary { method: sub-Fields() }

        Args:
            include_parent (bool): is the parent field included in the list of
                returned Fields (1st part of the tuple).
            current_path (list): the path of fields that will be skipped at the beginning
            path_shortcuts (bool): do we allow shortcuts in the path

        Returns:
            fields
            map_method_subfields
            map_original_field
            rename

        Example path = ROOT.A.B
        split_subfields(A.B.C.D, A.B.C.D', current_path=[ROOT,A,B]) => (Fields(), { C: [D, D'] })
        split_subfields(A.E.B.C.D, A.E.B.C.D', current_path=[ROOT,A,B]) => (Fields(), { C: [D, D'] })
        """
        fields = Fields()
        map_method_subfields = dict()
        map_original_field   = dict()
        rename = dict()

        for original_field in self:
            # The current_path can be seen as a set of fields that have to be
            # passed through before we can consider a field
            field, last = Fields.after_path(original_field, current_path, allow_shortcuts)

            field, _, subfield = field.partition(FIELD_SEPARATOR)

            if not subfield:
                fields.add(field)
            else:
                if include_parent:
                    fields.add(field)
                if not field in map_method_subfields:
                    map_method_subfields[field] = Fields()
                map_method_subfields[field].add(subfield)

            map_original_field[field] = original_field
            rename[field] = last

        return (fields, map_method_subfields, map_original_field, rename)
