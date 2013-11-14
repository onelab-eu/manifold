import json
from sqlalchemy                 import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm             import relationship, backref

try:
    from sfa.trust.credential   import Credential
except: pass
from manifold.util.predicate    import Predicate
from manifold.models            import Base, db
from manifold.models.user       import User
from manifold.models.platform   import Platform

class LinkedAccount(Base):
    __tablename__ = "linked_account"

    platform_id = Column(Integer, ForeignKey('platform.platform_id'), primary_key=True, doc='Platform identifier')
    user_id = Column(Integer, ForeignKey('user.user_id'), primary_key=True, doc='User identifier')

    identifier = Column(String, doc="Identifier")

    user = relationship("User", uselist = False)
    platform = relationship("Platform", uselist = False)
