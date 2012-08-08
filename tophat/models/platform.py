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
    config = Column(String, doc="Default configuration (serialized in JSON)")
