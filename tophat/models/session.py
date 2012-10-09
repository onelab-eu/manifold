from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref

from tophat.models import Base

import logging
log = logging.getLogger(__name__)

class Session(Base):
    session = Column(String, primary_key=True, doc="Session identifier")
    expires = Column(Integer, doc="Session identifier")
    user_id = Column(Integer, ForeignKey('user.user_id'), doc='User of the session')
    user = relationship("User", backref="sessions", uselist = False)
