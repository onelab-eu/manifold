from sqlalchemy                 import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm             import relationship, backref
from sfa.trust.credential       import Credential
from manifold.util.predicate    import Predicate
from manifold.util.predicate    import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains
from manifold.models            import Base, User, Platform, db
import json
import logging

log = logging.getLogger(__name__)

class Account(Base):

    user_filter = True

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
                print "PF=", pf
                assert pf.op == eq, "Only == is supported for convenience filter 'platform'"
                ret = db.query(Platform.platform_id)
                ret = ret.filter(Platform.platform == pf.value)
                ret = ret.one()
                filters.add(Predicate('platform_id', '=', ret[0]))
        return filters
        
    @staticmethod
    def process_params(params, filters, user):

        # PARAMS
# TODO        user_filters = filters.get('user')
# TODO        filters.delete('user')
# TODO        if user_filters:
# TODO            for uf in user_filters:
# TODO                assert uf.op == eq, "Only == is supported for convenience filter 'user'" 
# TODO                ret = db.query(User.user_id)
# TODO                ret = ret.filter(User.email == uf.value)
# TODO                ret = ret.one()
# TODO                filters.add(Predicate('user_id', '=', ret[0]))
# TODO            
# TODO        platform_filters = filters.get('platform')
# TODO        filters.delete('platform')
# TODO        if platform_filters:
# TODO            for pf in platform_filters:
# TODO                print "PF=", pf
# TODO                assert pf.op == eq, "Only == is supported for convenience filter 'platform'"
# TODO                ret = db.query(Platform.platform_id)
# TODO                ret = ret.filter(Platform.platform == pf.value)
# TODO                ret = ret.one()
# TODO                filters.add(Predicate('platform_id', '=', ret[0]))
# TODO        return filters

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in Account.__table__.columns])
        given_json_fields = given - accepted

        print "PARAMS:", params
        
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
                    c = Credential(string=params[field])
                    c_type = c.get_gid_object().get_type()
                    new_field = '%s_credential' % c_type
                    json_fields[new_field] = params[field]
                else:
                    json_fields[field] = params[field]
                del params[field]

            params['config'] = json.dumps(json_fields)

        return params
