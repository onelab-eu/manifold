#
# Functions for interacting with the persons table in the database
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# $Id: Persons.py 500 2007-06-11 09:16:05Z thierry $
#

from types import StringTypes
from datetime import datetime
import time
from random import Random
import re
import crypt

from tophat.util.parameter import Parameter
from tophat.core.filter import Filter
from tophat.core.table import Table, Row
from tophat.core.metadata.method import Method

class Field(Row):
    """
    Representation of a row in the field table. To use, optionally
    instantiate with a dict of values. Update as you would a
    dict. Commit to the database with sync().
    """

    table_name = 'fields'
    table_name_query = 'view_fields'
    primary_key = 'field_id'
    secondary_key = 'field'

    # tables which have references: cascades the deletion
    join_tables = []

    fields = {
        'field_id': Parameter(int, "field identifier"),
        'field': Parameter(str, "field name"),
        'header': Parameter(str, "column header"),
        'title': Parameter(str, "title"),
        'description': Parameter(str, "description"),
        'resource_type': Parameter(str, ""),
        'info_type': Parameter(str, ""),
        'value_type': Parameter(str, ""),
        'unit': Parameter(str, ""),
        'last_fetched': Parameter(int, "Time last fetched"),
        'allowed_values': Parameter(str, "String representing the set of accepted values")
    }

    exported_fields = ['field_id', 'field', 'header', 'title', 'description', 'resource_type', 'info_type', 'value_type', 'unit', 'last_fetched', 'allowed_values']
    view_fields = Row.add_exported_fields(fields, [Method])

    # for Cache
    class_key = 'field_id'
    foreign_fields = []

    def delete(self, commit = True):
        """
        Delete existing field.
        """
        assert 'field_id' in self

            # Clean up miscellaneous join tables
        for table in self.join_tables:
             self.api.db.do("DELETE FROM %s WHERE field_id = %d" % \
                (table, self['field_id']))

        # Mark as deleted
        self['deleted'] = True
        self.sync(commit)
    
class Fields(Table):
    """
    Representation of row(s) from the field table in the
    database.
    """

    def __init__(self, api, field_filter = None, columns = None):
        Table.__init__(self, api, MetadataField, field_filter, columns)
