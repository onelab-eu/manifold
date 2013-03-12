from elixir import (Field, Unicode, Integer, Entity, Boolean,
        using_table_options, ManyToOne)

from manifold.models.base import BaseEntityMixin
from tophat.conf import settings

import logging
log = logging.getLogger(__name__)

#from types import StringTypes
#from datetime import datetime
#import time
#from random import Random
#import re
#import crypt
#
#from tophat.util.parameter import Parameter
#from tophat.core.filter import Filter
#from tophat.core.table import Table, Row
#from tophat.core.metadata.platform import Platform

class Gateway(BaseEntityMixin, Entity):
    gateway_id = Field(Integer, primary_key=True, doc="Gateway identifier")
    platform = ManyToOne('Platform')
    config = Field(Unicode)

#    table_name = 'gateways'
#    table_name_query = 'view_gateways'
#    #table_name_query = 'view_metadata_gateways'
#    primary_key = 'gateway_id'
#    secondary_key = 'gateway'
#
#    # tables which have references: cascades the deletion
#    join_tables = []
#
#    fields = {
#        'gateway_id': Parameter(int, "gateway identifier"),
#        'platform_id': Parameter(int, "platform identifier"),
#        'config': Parameter(str, "gateway name")
#    }
#
#    exported_fields = ['gateway_id', 'gateway']
#    view_fields = Row.add_exported_fields(fields, [Platform])
#
#    # for Cache
#    class_key = 'gateway_id'
#    foreign_fields = []
#
#    def delete(self, commit = True):
#        """
#        Delete existing field.
#        """
#        assert 'gateway_id' in self
#
#            # Clean up miscellaneous join tables
#        for table in self.join_tables:
#             self.api.db.do("DELETE FROM %s WHERE gateway_id = %d" % \
#                (table, self['gateway_id']))
#
#        # Mark as deleted
#        self['deleted'] = True
#        self.sync(commit)
#    
#class Gateways(Table):
#    """
#    Representation of row(s) from the gateway table in the
#    database.
#    """
#
#    def __init__(self, api, gateway_filter = None, columns = None):
#        Table.__init__(self, api, MetadataGateway, gateway_filter, columns)
