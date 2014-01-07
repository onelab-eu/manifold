import json
from sqlalchemy                 import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm             import relationship, backref

try:
    from sfa.trust.credential   import Credential
except: pass
from manifold.util.predicate    import Predicate
from manifold.models            import Base
from manifold.models.user       import User
from manifold.models.platform   import Platform

class Policy(Base):
    policy_id = Column(Integer, primary_key=True, doc="Policy rule identifier")
    policy_json = Column(String, doc="Policy rule in JSON format")
