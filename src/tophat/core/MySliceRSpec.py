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
from tophat.core.table import Row, Table

class MySliceRSpec(Row):
    """
    Representation of a row in the rspec table. To use, optionally
    instantiate with a dict of values. Update as you would a
    dict. Commit to the database with sync().
    """

    table_name = 'rspec'
    primary_key = 'rspec_id'

    # tables which have references: cascades the deletion
    join_tables = []

    fields = {
        'rspec_id': Parameter(int, "rspec identifier"),
        'rspec_person_id': Parameter(int, "Identifier of the person to whom the rspec belongs"),
        'rspec_hash': Parameter(str, "Hash of the rspec"),
        'rspec_target': Parameter(str, "HRN of the target slice"),
        'rspec_expiration': Parameter(int, "Expiration, in seconds since UNIX epoch"),
        'rspec': Parameter(str, "RSpec string")
    }

    exported_fields = ['rspec_id', 'rspec_person_id', 'rspec_hash', 'rspec_target', 'rspec_expiration', 'rspec']
    view_fields = fields

    # for Cache
    class_key = 'rspec_id'
    foreign_fields = []

    def delete(self, commit = True):
        """
        Delete existing rspec.
        """
        assert 'rspec_id' in self

            # Clean up miscellaneous join tables
        for table in self.join_tables:
             self.api.db.do("DELETE FROM %s WHERE rspec_id = %d" % \
                (table, self['rspec_id']))

        # Mark as deleted
        self['deleted'] = True
        self.sync(commit)

class MySliceRSpecs(Table):
    """
    Representation of row(s) from the rspec table in the
    database.
    """

    def __init__(self, api, rspec_filter = None, columns = None):
        Table.__init__(self, api, MySliceRSpec, columns)

        sql = "SELECT %s FROM rspec WHERE True" % \
               ", ".join(self.columns)
        #deleted IS False" % \

        if rspec_filter is not None:
            if isinstance(rspec_filter, (list, tuple, set)):
                # Separate the list into integers and strings
                ints = filter(lambda x: isinstance(x, (int, long)), rspec_filter)
                strs = filter(lambda x: isinstance(x, StringTypes), rspec_filter)
                rspec_filter = Filter(MySliceRSpec.fields, {'rspec_id': ints, 'name': strs})
                sql += " AND (%s) %s" % rspec_filter.sql(api, "OR")
            elif isinstance(rspec_filter, dict):
                rspec_filter = Filter(MySliceRSpec.fields, rspec_filter)
                sql += " AND (%s) %s" % rspec_filter.sql(api, "AND")
            elif isinstance (rspec_filter, StringTypes):
                rspec_filter = Filter(MySliceRSpec.fields, {'name':[rspec_filter]})
                sql += " AND (%s) %s" % rspec_filter.sql(api, "AND")
            elif isinstance (rspec_filter, int):
                rspec_filter = Filter(MySliceRSpec.fields, {'rspec_id':[rspec_filter]})
                sql += " AND (%s) %s" % rspec_filter.sql(api, "AND")
            else:
                raise PLCInvalidArgument, "Wrong rspec filter %r"%rspec_filter

        self.selectall(sql)
