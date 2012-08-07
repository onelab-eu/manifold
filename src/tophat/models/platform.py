from elixir import (Field, Text, Integer, Entity, Boolean,
        using_table_options, ManyToOne)

from tophat.models.base import BaseEntityMixin
from tophat.conf import settings

import logging
log = logging.getLogger(__name__)


#from types import StringTypes
#from datetime import datetime
#import md5
#import time
#from random import Random
#import re
#import crypt
#
#from tophat.util.faults import *
#from tophat.core.debug import log
#from tophat.util.parameter import Parameter
#from tophat.core.filter import Filter
#from tophat.core.table import Row, Table
#from tophat.core.roles import Role, Roles
#
#from tophat.core.keys import Key, Keys
#from tophat.core.messages import Message, Messages

class Platform(BaseEntityMixin, Entity):
    platform_id = Field(Integer, primary_key=True, doc="Platform identifier")
    platform = Field(Text, doc="Platform name")
    platform_longname = Field(Text, doc="Platform long name")
    platform_description = Field(Text, doc="Platform description")
    platform_url = Field(Text, doc="Platform URL")
    deleted = Field(Boolean, default=False, doc="Platform has been deleted")
    disabled = Field(Boolean, default=False, doc="Platform has been disabled")
    status = Field(Text, doc="Platform status")
    status_updated = Field(Integer, doc="Platform last check")
    platform_has_agents = Field(Boolean, default=False, doc="Platform has agents")
    first = Field(Integer, doc="First timestamp, in seconds since UNIX epoch")
    last = Field(Integer, doc="Last timestamp, in seconds since UNIX epoch")

#    def gateways(self):
#        """
#        Get associated gateways
#        """
#        assert 'platform_id' in self
#        return Gateways(self.api, {'platform_id': self['platform_id']})    
#
#    def delete(self, commit = True):
#        """
#        Delete existing platform.
#        """
#        assert 'platform_id' in self
#
#        # Clean up miscellaneous join tables
#        for table in self.join_tables:
#            self.api.db.do("DELETE FROM %s WHERE platform_id = %d" % \
#                    (table, self['platform_id']))
#
#        # Mark as deleted
#        self['deleted'] = True
#        self.sync(commit)
#
#class Platforms(Table):
#    """
#    Representation of row(s) from the platform table in the
#    database.
#    """
#
#    def __init__(self, api, platform_filter = None, columns = None):
#        Table.__init__(self, api, Platform, platform_filter, columns)
