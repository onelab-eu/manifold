from sqlalchemy import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm import relationship, backref

from manifold.models import Base, User, Platform, db
import json

import logging
log = logging.getLogger(__name__)

class Account(Base):
    platform_id = Column(Integer, ForeignKey('platform.platform_id'), primary_key=True, doc='Platform identifier')
    user_id = Column(Integer, ForeignKey('user.user_id'), primary_key=True, doc='User identifier')
    auth_type = Column(Enum('none', 'default', 'user', 'reference', 'managed'), default='default')
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
        if not gtype:
            print "I: Undefined gateway"
            return {}
        gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)

        print "I: Calling manage on the platform"
        config = json.dumps(gw.manage(self.user, self.platform, json.loads(self.config)))
        if self.config != config:
            self.config = config
            db.commit()

    def config_set(self, value):
        self.config = json.dumps(value)
        db.add(self)
        db.commit()
        
    def config_get(self):
        return json.loads(self.config)

    @staticmethod
    def process_params(params):
        if 'user' in params:
            ret = db.query(User.user_id).filter(User.email == params['user']).one()
            params['user_id'] = ret[0]
            del params['user']
            
        if 'platform' in params:
            ret = db.query(Platform.platform_id).filter(Platform.platform == params['platform']).one()
            params['platform_id'] = ret[0]
            del params['platform']

        return params
