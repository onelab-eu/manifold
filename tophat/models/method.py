#from elixir import (Field, Unicode, Integer, Entity, Boolean,
#        using_table_options, ManyToOne)
#
#from tophat.models.base import BaseEntityMixin
#from tophat.conf import settings
#
#import logging
#log = logging.getLogger(__name__)
#
##from types import StringTypes
##from datetime import datetime
##import time
##from random import Random
##import re
##import crypt
##
##from tophat.util.parameter import Parameter
##from tophat.core.filter import Filter
##from tophat.core.table import Table, Row
#
#class Method(BaseEntityMixin, Entity):
#    method_id = Field(Integer, primary_key=True, doc="Method identifier")
#    platform = ManyToOne('Platform')
#    method = Field(Unicode, doc="Method name")
#
##    table_name = 'methods'
##    table_name_query = 'view_methods'
##    #table_name_query = 'view_metadata_methods'
##    primary_key = 'method_id'
##    secondary_key = 'method'
##
##    # tables which have references: cascades the deletion
##    join_tables = []
##
##    fields = {
##        'method_id': Parameter(int, "method identifier"),
##        'platform_id': Parameter(int, "platform identifier"),
##        'method': Parameter(str, "method name")
##    }
##
##    exported_fields = ['method_id', 'method']
##    view_fields = fields
##
##    # for Cache
##    class_key = 'method_id'
##    foreign_fields = []
##
##    def delete(self, commit = True):
##        """
##        Delete existing field.
##        """
##        assert 'method_id' in self
##
##            # Clean up miscellaneous join tables
##        for table in self.join_tables:
##             self.api.db.do("DELETE FROM %s WHERE method_id = %d" % \
##                (table, self['method_id']))
##
##        # Mark as deleted
##        self['deleted'] = True
##        self.sync(commit)
##    
##class Methods(Table):
##    """
##    Representation of row(s) from the method table in the
##    database.
##    """
##
##    def __init__(self, api, method_filter = None, columns = None):
##        Table.__init__(self, api, MetadataMethod, method_filter, columns)
