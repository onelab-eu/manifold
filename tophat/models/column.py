##
## Functions for interacting with the persons table in the database
##
## Mark Huang <mlhuang@cs.princeton.edu>
## Copyright (C) 2006 The Trustees of Princeton University
##
## $Id: Persons.py 500 2007-06-11 09:16:05Z thierry $
##
#
#from types import StringTypes
#from datetime import datetime
#import time
#from random import Random
#import re
#import crypt
#
#from tophat.core.parameter import Parameter
#from tophat.core.filter import Filter
#from tophat.core.table import Table, Row
#from tophat.core.method import Method
#
#class MetadataColumn(Row):
#    """
#    Representation of a row in the column table. To use, optionally
#    instantiate with a dict of values. Update as you would a
#    dict. Commit to the database with sync().
#    """
#
#    table_name = None
#    table_name_query = 'view_columns'
#    primary_key = 'column'
#    secondary_key = 'filter'
#
#    # tables which have references: cascades the deletion
#    join_tables = []
#
#    fields = {
#        'table': Parameter(str, "table"),
#        'column': Parameter(str, "column"),
#        'platform_ids': Parameter(str, "platform ids")
#    }
#
#    exported_columns = []
#    view_fields = fields
#
#    # for Cache
#    class_key = 'column_id'
#    foreign_columns = []
#    
#class MetadataColumns(Table):
#    """
#    Representation of row(s) from the column table in the
#    database.
#    """
#
#    def __init__(self, api, column_filter = None, columns = None):
#        Table.__init__(self, api, MetadataColumn, column_filter, columns)
