from sqlalchemy import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm import relationship, backref

from tophat.models import Base, session
import json

import logging
log = logging.getLogger(__name__)

class Account(Base):
    platform_id = Column(Integer, ForeignKey('platform.platform_id'), primary_key=True, doc='Platform identifier')
    user_id = Column(Integer, ForeignKey('user.user_id'), primary_key=True, doc='User identifier')
    auth_type = Column(Enum('none', 'default', 'user', 'managed'), default='default')
    config = Column(String, doc="Default configuration (serialized in JSON)")

    user = relationship("User", backref="accounts", uselist = False)
    platform = relationship("Platform", backref="platforms", uselist = False)

    def manage(self):
        """
        Ensure that the config has all the necessary fields
        """
        assert self.auth_type == 'managed'

        # Finds the gateway corresponding to the platform
        gtype = self.platform.gateway_type
        print self.platform.platform
        print gtype
        if not gtype:
            print "I: Undefined gateway"
            return {}
        gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)

        print "I: Calling manage on the platform"
        config = json.dumps(gw.manage(self.user, self.platform, json.loads(self.config)))
        if self.config != config:
            self.config = config
            session.commit()

    def config_set(self, value):
        self.config = json.dumps(value)
        session.add(self)
        session.commit()
        
    def config_get(self):
        return json.loads(self.config)

