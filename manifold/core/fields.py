# -*- coding: utf-8 -*-

# The distinction between parent and children fields is based on the
# Fields.FIELD_SEPARATOR character.
#
# See manifold.operators.subquery
FIELD_SEPARATOR = '.'

class Fields(set):
    def iter_field_subfield(self):
        for f in self:
            field, _, subfield = f.partition(FIELD_SEPARATOR)
            yield (field, subfield)
