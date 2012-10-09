from sqlalchemy import Column, Integer, String

from tophat.models import Base

import logging
log = logging.getLogger(__name__)

class Session(Base):
    session = Column(Integer, primary_key=True, doc="Session identifier")
    expires = Column(Integer, doc="Session identifier")
    user_id = Column(Integer, ForeignKey('user.user_id'), doc='User of the session')
    user = relationship("User", backref="accounts", uselist = False)
    

