from sqlalchemy import Column, Integer, String

from tophat.models import Base

import logging
log = logging.getLogger(__name__)

class User(Base):
    user_id = Column(Integer, primary_key=True, doc="User identifier")
    email = Column(String, doc="User email")
    password = Column(String, doc="User password")
    

