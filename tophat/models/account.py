from sqlalchemy import Column, Integer, String, Enum

from tophat.models import Base

import logging
log = logging.getLogger(__name__)

class Account(Base):
    platform_id = Column(Integer, primary_key=True, doc='Platform identifier')
    user_id = Column(Integer, primary_key=True, doc='User identifier')
    auth_type = Column(Enum('none', 'default', 'user', 'managed'), default='default')
    config = Column(String, doc="Default configuration (serialized in JSON)")
