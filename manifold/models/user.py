from sqlalchemy import Column, Integer, String

from manifold.models import Base

class User(Base):
    restrict_to_self = True
    user_id = Column(Integer, primary_key=True, doc="User identifier")
    email = Column(String, doc="User email")
    password = Column(String, doc="User password")
    config = Column(String, doc="User password")

