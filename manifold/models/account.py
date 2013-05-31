from sqlalchemy                 import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm             import relationship, backref
from sfa.trust.credential       import Credential
from manifold.util.predicate    import Predicate
from manifold.util.predicate    import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains
from manifold.models            import Base, User, Platform, db
import json

class Account(Base):

    restrict_to_self = True

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
        gw = getattr(__import__('manifold.gateways', globals(), locals(), gtype), gtype)

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
    def process_filters(filters):
        user_filters = filters.get('user')
        filters.delete('user')
        if user_filters:
            for uf in user_filters:
                assert uf.op == eq, "Only == is supported for convenience filter 'user'" 
                ret = db.query(User.user_id)
                ret = ret.filter(User.email == uf.value)
                ret = ret.one()
                filters.add(Predicate('user_id', '=', ret[0]))
            
        platform_filters = filters.get('platform')
        filters.delete('platform')
        if platform_filters:
            for pf in platform_filters:
                assert pf.op == eq, "Only == is supported for convenience filter 'platform'"
                ret = db.query(Platform.platform_id)
                ret = ret.filter(Platform.platform == pf.value)
                ret = ret.one()
                filters.add(Predicate('platform_id', '=', ret[0]))
        return filters
        
    @staticmethod
    def process_params(params, filters, user):

        # PARAMS 
        # added by Loic, based on the process_filters functions
        user_params = params.get('user')
        if user_params:
            del params['user']
            #print "user_params=",user_params
            ret = db.query(User.user_id)
            ret = ret.filter(User.email == user_params)
            ret = ret.one()
            params['user_id']=ret[0]

        platform_params = params.get('platform')
        if platform_params:
            del params['platform']
            #print "platform_params=", platform_params
            ret = db.query(Platform.platform_id)
            ret = ret.filter(Platform.platform == platform_params)
            ret = ret.one()
            params['platform_id']=ret[0]

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in Account.__table__.columns])
        given_json_fields = given - accepted
        
        if given_json_fields:
            if 'config' in given_json_fields:
                raise Exception, "Cannot mix full JSON specification & JSON encoded fields"

            r = db.query(Account.config).filter(filters)
            if user:
                r = r.filter(Account.user_id == user.user_id)
            r = r.filter(filters) #Account.platform_id == platform_id)
            r = r.one()
            try:
                json_fields = json.loads(r.config)
            except Exception, e:
                json_fields = {}

            # We First look at convenience fields
            for field in given_json_fields:
                if field == 'credential':
                    # We'll determine the type of credential
                    # XXX NOTE This is SFA specific... it should be hooked by gateways
                    # @loic modified according to the SFA Gateway, to handle delegation
                    # XXX TODO need to be improved...
                    c = Credential(string=params[field])
                    c_type = c.get_gid_object().get_type()
                    if c_type == 'user':
                        new_field = 'delegated_%s_credential' % c_type
                        json_fields[new_field] = params[field]
                    else: 
                        cred_name='delegated_%s_credentials'% c_type
                        if not cred_name in json_fields:
                            json_fields[cred_name] = {}
                        c_target = c.get_gid_object().get_hrn()
                        json_fields[cred_name][c_target] = params[field]
                else:
                    json_fields[field] = params[field]
                del params[field]

            params['config'] = json.dumps(json_fields)

        return params
