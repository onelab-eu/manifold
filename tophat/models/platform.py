from sqlalchemy import Column, Integer, String, Boolean, Enum

from tophat.models import Base

import logging
log = logging.getLogger(__name__)


class Platform(Base):
    platform_id = Column(Integer, primary_key=True, doc="Platform identifier")
    platform = Column(String, doc="Platform name")
    platform_longname = Column(String, doc="Platform long name")
    platform_description = Column(String, doc="Platform description")
    platform_url = Column(String, doc="Platform URL")
    deleted = Column(Boolean, default=False, doc="Platform has been deleted")
    disabled = Column(Boolean, default=False, doc="Platform has been disabled")
    status = Column(String, doc="Platform status")
    status_updated = Column(Integer, doc="Platform last check")
    platform_has_agents = Column(Boolean, default=False, doc="Platform has agents")
    first = Column(Integer, doc="First timestamp, in seconds since UNIX epoch")
    last = Column(Integer, doc="Last timestamp, in seconds since UNIX epoch")

    gateway_type = Column(String, doc="Type of the gateway to use to connect to this platform")
    gateway_conf = Column(String, doc="Parameters of the gateway")
    auth_type = Column(Enum('none', 'default', 'user', 'managed'), default='default')
    auth_default = Column(String, doc="Default configuration (serialized in JSON (?)")

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
