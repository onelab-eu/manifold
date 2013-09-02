from __future__                 import absolute_import
from sqlalchemy                 import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm             import sessionmaker
from manifold.gateways          import Gateway
from manifold.models            import *
from manifold.util.log          import Log
from manifold.util.predicate    import included

import traceback
import json
import sys
from hashlib import md5
import time
from random import Random
import crypt

def xgetattr(cls, list_attr):
    ret = []
    for a in list_attr:
        ret.append(getattr(cls,a))
    return tuple(ret)

def get_sqla_filters(cls, filters):
    if filters:
        _filters = []
        for p in filters:
            if p.op == included:
                f = getattr(cls, p.key).in_(p.value)
            else:
                f = p.op(getattr(cls, p.key), p.value)
            _filters.append(f)
        return _filters
    else:
        return None

def row2dict(row):
    try:
        return {c.name: getattr(row, c.name) for c in row.__table__.columns}
    except:
        Log.tmp("Inconsistency in ROW2DICT")
        return {c: getattr(row, c) for c in row.keys()}

class SQLAlchemyGateway(Gateway):

    map_object = {
        'platform' : Platform,
        'user'     : User,
        'account'  : Account,
        'session'  : Session
    }

    def __init__(self, router=None, platform=None, query=None, config=None, user_config=None, user=None, format='dict'):

        assert format in ['dict', 'object'], 'Unknown return format for gateway SQLAlchemy'
        if format == 'object':
            Log.tmp("Objects should not be used")
        self.format = format

        super(SQLAlchemyGateway, self).__init__(router, platform, query, config, user_config, user)

        engine = create_engine(config['url'], echo=False)
        
        class Base(object):
            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()
        
            __mapper_args__= {'always_refresh': True}
        
            #id =  Column(Integer, primary_key=True)
        
            #def to_dict(self):
            #    return {c.name: getattr(self, c.name) for c in self.__table__.columns}
        
            #@staticmethod
            #def process_params(params):
            #    return params
        
        Base = declarative_base(cls=Base)
        
        Session = sessionmaker(bind=engine)
        self.db = Session()
        
        # Models
        from manifold.models.platform   import Platform as DBPlatform 
        from manifold.models.user       import User     as DBUser
        from manifold.models.account    import Account  as DBAccount
        from manifold.models.session    import Session  as DBSession
        
        Base.metadata.create_all(engine)
        
        # Create a session
        #Session = sessionmaker()
        #Session.configure(bind=engine)

    def local_query_get(self, query):
        #
        # XXX How are we handling subqueries
        #

        fields = query.fields
        # XXX else tap into metadata

        cls = self.map_object[query.object]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.fields) if query.fields else None


        res = db.query( *_fields ) if _fields else db.query( cls )
        if query.filters:
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if cls.restrict_to_self and self.user.email != 'demo':
                res = res.filter(cls.user_id == self.user.user_id)
        except AttributeError: pass

        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]

        return tuplelist

    def local_query_update(self, query):

        # XXX The filters that are accepted in config depend on the gateway
        # Real fields : user_credential
        # Convenience fields : credential, then redispatched to real fields
        # same for get, update, etc.

        # XXX What about filters on such fields
        # filter works with account and platform table only
        if query.object == 'account' or query.object == 'platform':
            if not query.filters.has_eq('platform_id') and not query.filters.has_eq('platform'):
                raise Exception, "Cannot update JSON fields on multiple platforms"

        cls = self.map_object[query.object]

        # Note: we can request several values

        # FIELDS: exclude them
        _fields = xgetattr(cls, query.fields)


        # FILTERS: Note we cannot filter on json fields
        _filters = cls.process_filters(query.filters)
        _filters = get_sqla_filters(cls, _filters)

        # PARAMS
        #
        # The fields we can update in params are either:
        # - the original fields, including json encoded ones
        # - fields inside the json encoded ones
        # - convenience fields
        # We refer to the model for transforming the params structure into the
        # final one
    
        # Password update
        #
        # if there is password update in query.params
        # We encrypt the password according to the encryption of adduser.py
        # As a result from the frontend the edited password will be inserted
        # into the local DB as encrypted      
        if 'password' in query.params:
            query.params['password'] = self.encrypt_password(query.params['password'])
        _params = cls.process_params(query.params, _filters, self.user)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in _params.items() ])
       
        #db.query(cls).update(_params, synchronize_session=False)
        q = db.query(cls)
        for _filter in _filters:
            q = q.filter(_filter)
        if cls.restrict_to_self:
            q = q.filter(getattr(cls, 'user_id') == self.user.user_id)
        q = q.update(_params, synchronize_session=False)
        db.commit()

        return []

    def local_query_create(self, query):

        assert not query.filters, "Filters should be empty for a create request"
        #assert not query.fields, "Fields should be empty for a create request"

        cls = self.map_object[query.object]

        params = query.params
        # We encrypt the password according to the encryption of adduser.py
        # As a result from the frontend the new users' password will be inserted
        # into the local DB as encrypted      
        if 'password' in params:
            params['password'] = self.encrypt_password(params['password'])
        
        cls.process_params(query.params, None, self.user)
        new_obj = cls(**params) if params else cls()
        db.add(new_obj)
        db.commit()
        
        return [new_obj]

    def encrypt_password(self, password):
        #
        # password encryption taken from adduser.py 
        # 

        magic = "$1$"
        password = password
        # Generate a somewhat unique 8 character salt string
        salt = str(time.time()) + str(Random().random())
        salt = md5(salt).hexdigest()[:8]

        if len(password) <= len(magic) or password[0:len(magic)] != magic:
            password = crypt.crypt(password.encode('latin1'), magic + salt + "$")
    
        return password 

    def start(self):
        assert self.query, "Cannot start gateway with no query associated"
        _map_action = {
            "get"    : self.local_query_get,
            "update" : self.local_query_update,
            "create" : self.local_query_create
        }
        table = _map_action[self.query.action](self.query)
        # XXX For local namespace queries, we need to keep a dict
        for t in table:
#MANDO|            row = row2dict(t) if self.format == 'dict' else t.get_object()
            row = row2dict(t) if self.format == 'dict' else t
            self.callback(row)
        self.callback(None)

    def get_metadata(self):
        return []	
