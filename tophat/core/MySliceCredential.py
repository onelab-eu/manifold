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

class MySliceCredential(Row):
    """
    Representation of a row in the credential table. To use, optionally
    instantiate with a dict of values. Update as you would a
    dict. Commit to the database with sync().
    """

    table_name = 'credential'
    primary_key = 'credential_id'

    # tables which have references: cascades the deletion
    join_tables = []

    fields = {
        'credential_id': Parameter(int, "credential identifier"),
        'credential_person_id': Parameter(int, "Identifier of the person to whom the credential belongs"),
        'credential_type': Parameter(str, "HRN type"),
        'credential_target': Parameter(str, "HRN of the slice"),
        'credential_expiration': Parameter(int, "Expiration, in seconds since UNIX epoch"),
        'credential': Parameter(str, "Credential string")
    }

    exported_fields = ['credential_id', 'credential_person_id', 'credential_type', 'credential_target', 'credential_expiration', 'credential']
    view_fields = fields

    # for Cache
    class_key = 'credential_id'
    foreign_fields = []

    def delete(self, commit = True):
        """
        Delete existing credential.
        """
        assert 'credential_id' in self

            # Clean up miscellaneous join tables
        for table in self.join_tables:
             self.api.db.do("DELETE FROM %s WHERE credential_id = %d" % \
                (table, self['credential_id']))

        # Mark as deleted
        self['deleted'] = True
        self.sync(commit)
    
class MySliceCredentials(Table):
    """
    Representation of row(s) from the credential table in the
    database.
    """

    def __init__(self, api, credential_filter = None, columns = None):
        Table.__init__(self, api, MySliceCredential, columns)

        sql = "SELECT %s FROM credential WHERE True" % \
               ", ".join(self.columns)
        #deleted IS False" % \

        if credential_filter is not None:
            if isinstance(credential_filter, (list, tuple, set)):
                # Separate the list into integers and strings
                ints = filter(lambda x: isinstance(x, (int, long)), credential_filter)
                strs = filter(lambda x: isinstance(x, StringTypes), credential_filter)
                credential_filter = Filter(MySliceCredential.fields, {'credential_id': ints, 'name': strs})
                sql += " AND (%s) %s" % credential_filter.sql(api, "OR")
            elif isinstance(credential_filter, dict):
                credential_filter = Filter(MySliceCredential.fields, credential_filter)
                sql += " AND (%s) %s" % credential_filter.sql(api, "AND")
            elif isinstance (credential_filter, StringTypes):
                credential_filter = Filter(MySliceCredential.fields, {'name':[credential_filter]})
                sql += " AND (%s) %s" % credential_filter.sql(api, "AND")
            elif isinstance (credential_filter, int):
                credential_filter = Filter(MySliceCredential.fields, {'credential_id':[credential_filter]})
                sql += " AND (%s) %s" % credential_filter.sql(api, "AND")
            else:
                raise PLCInvalidArgument, "Wrong credential filter %r"%credential_filter

        self.selectall(sql)
