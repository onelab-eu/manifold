from sqlalchemy      import Column, Integer, String
from manifold.models import Base, db

import json

class User(Base):
    restrict_to_self = True
    user_id = Column(Integer, primary_key=True, doc="User identifier")
    email = Column(String, doc="User email")
    password = Column(String, doc="User password")
    config = Column(String, doc="User config (serialized in JSON)")

    def config_set(self, value):
        #Log.deprecated()
        return self.set_config(value)
        
    def set_config(self, value):
        self.config = json.dumps(value)
        db.add(self)
        db.commit()
        
    def config_get(self):
        #Log.deprecated()
        return self.get_config()

    def get_config(self):
        if not self.config:
            return {}
        return json.loads(self.config)

    @staticmethod
    def process_params(params, filters, user):

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in User.__table__.columns])
        given_json_fields = given - accepted
        
        if given_json_fields:
            if 'config' in given_json_fields:
                raise Exception, "Cannot mix full JSON specification & JSON encoded fields"

            r = db.query(User.config).filter(filters)
            if user:
                r = r.filter(User.user_id == user.user_id)
            r = r.filter(filters) #User.platform_id == platform_id)
            r = r.one()
            try:
                json_fields = json.loads(r.config)
            except Exception, e:
                json_fields = {}

            # We First look at convenience fields
            for field in given_json_fields:
                json_fields[field] = params[field]
                del params[field]

            params['config'] = json.dumps(json_fields)

        return params

    @classmethod
    def params_ensure_user(cls, params, user):
        # A user can only create its own objects
        if cls.restrict_to_self:
            params['user_id'] = user.user_id
            return

        if 'user_id' in params: return
        if 'user' in params:
            user_params = params['user']
            del params['user']
            ret = db.query(User.user_id)
            ret = ret.filter(User.email == user_params)
            ret = ret.one()
            params['user_id']=ret[0]
            return
        raise Exception, 'User should be specified'
 
