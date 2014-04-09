# -*- coding: utf-8 -*-

from types                          import StringTypes

# The distinction between parent and children fields is based on the
# Fields.FIELD_SEPARATOR character.
#
# See manifold.operators.subquery
FIELD_SEPARATOR = '.'
DEFAULT_IS_STAR = False

class Fields(set):
    """
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        star = kwargs.pop('star', DEFAULT_IS_STAR)
        set.__init__(self, *args, **kwargs)
        self._star = False if set(self) else star

    def __repr__(self):
        if self.is_star():
            return "<Fields *>"
        else:
            return "<Fields %r>" % [x for x in self]

    def __str__(self):
        if self.is_star():
            return "<*>"
        else:
            return "<%r>" % [x for x in self]

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def is_star(self):
        return self._star

    def set_star(self):
        self._star = True
        set.clear(self)

    def unset_star(self, fields = None):
        self._star = False
        if fields:
            self |= fields

    def is_empty(self):
        return not self.is_star() and not self

    def copy(self):
        return Fields(set.copy(self))

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

    def __or__(self, fields):
        if self.is_star() or fields.is_star():
            return Fields(star = True)
        else:
            return Fields(set.__or__(self, fields))

    def __ior__(self, fields):
        if self.is_star() or fields.is_star():
            self.set_star()
            return self
        else:
            return set.__ior__(self, fields)

    def __and__(self, fields):
        if self.is_star():
            return fields.copy()
        elif fields.is_star():
            return self.copy()
        else:
            return Fields(set.__and__(self, fields))

    def __iand__(self, fields):
        if self.is_star():
            self.unset_star(fields)
        elif fields.is_star():
            pass
        else:
            set.__iand__(self, fields)

    def __nonzero__(self):
        return self.is_star() or bool(set(self))

    # Python>=3
    __bool__ = __nonzero__

    __add__ = __or__

    def __sub__(self, fields):
        if fields.is_star():
            return Fields(star = False)
        else:
            if self.is_star():
                # * - x,y,z = ???
                return Fields(star = True) # XXX NotImplemented
            else:
                return Fields(set.__sub__(self, fields))

    def __isub__(self, fields):
        raise NotImplemented

    def __iadd__(self, fields):
        raise NotImplemented

    #---------------------------------------------------------------------------
    # Overloaded set comparison functions
    #---------------------------------------------------------------------------

    def __eq__(self, other):
        return self.is_star() and other.is_star() or set.__eq__(self, other)

    def __le__(self, other):
        return self.is_star() and other.is_star() or set.__eq__(self, other)

    # Defined with respect of previous functions

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return other.__le__(self) 

    def __gt__(self, other):
        return other.__lt__(self)

    #---------------------------------------------------------------------------
    # Overloaded set functions
    #---------------------------------------------------------------------------

    def add(self, field_name):
        if not isinstance(field_name, StringTypes):
            raise TypeError("Invalid field_name name %s (string expected, got %s)" % (field_name, type(field_name)))

        if not self.is_star():
            set.add(self, field_name)
        
    def clear(self):
        self._star = False
        set.clear(self)

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
    def join(field, subfield):
        return "%s%s%s" % (field, FIELD_SEPARATOR, subfield)

    @staticmethod
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

    def split_subfields(self, include_parent = True, current_path = None, allow_shortcuts = True):
        """
        Returns a tuple of Fields + dictionary { method: sub-Fields() }

        Args:
            include_parent (bool): is the parent field included in the list of
                returned Fields (1st part of the tuple).
            current_path (list): the path of fields that will be skipped at the beginning
            path_shortcuts (bool): do we allow shortcuts in the path

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
