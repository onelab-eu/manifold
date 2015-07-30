from __future__                 import absolute_import
from sqlalchemy                 import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm             import sessionmaker
from sqlalchemy.exc             import SQLAlchemyError

from manifold.conf              import ADMIN_USER
from manifold.gateways          import Gateway
from manifold.util.log          import Log
from manifold.util.predicate    import included
from manifold.core.record       import Record, LastRecord

from manifold.models            import db
from manifold.models.account    import Account
#from manifold.models.linked_account    import LinkedAccount
from manifold.models.policy     import Policy
from manifold.models.platform   import Platform
from manifold.models.user       import User
from manifold.models.session    import Session as DBSession 

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

def row2record(row):
    try:
        return Record({c.name: getattr(row, c.name) for c in row.__table__.columns})
    except:
        #Log.tmp("Inconsistency in ROW2RECORD", row)
        return Record({c: getattr(row, c) for c in row.keys()})

class SQLAlchemyGateway(Gateway):
    __gateway_name__ = 'sqlalchemy'

    map_object = {
        'platform' : Platform,
        'user'     : User,
        'account'  : Account,
        'session'  : DBSession,
        #'linked_account': LinkedAccount,
        'policy'   : Policy
    }

    def __init__(self, router=None, platform=None, query=None, config=None, user_config=None, user=None, format='record'):

        assert format in ['record', 'object'], 'Unknown return format for gateway SQLAlchemy'
        if format == 'object':
            Log.tmp("Objects should not be used")
        self.format = format

        super(SQLAlchemyGateway, self).__init__(router, platform, query, config, user_config, user)

        from manifold.models.base import Base
        Base = declarative_base(cls=Base)
        
        # Models
        from manifold.models.platform   import Platform as DBPlatform 
        from manifold.models.user       import User     as DBUser
        from manifold.models.account    import Account  as DBAccount
        from manifold.models.session    import Session  as DBSession

        engine = create_engine(config['url'], echo=False, pool_recycle=3600)
        
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        self.db = Session()
        
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
            if self.user and cls.restrict_to_self and self.user['email'] != ADMIN_USER:
                res = res.filter(cls.user_id == self.user['user_id'])
        except AttributeError: pass
        try:
            tuplelist = res.all()
            return tuplelist
        except SQLAlchemyError, e:
            Log.error("SQLAlchemyError trying to rollback db session: %s" % e)
            db.rollback()
            self.local_query_get(query)
            return list()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]

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
        if self.user and cls.restrict_to_self and self.user['email'] != ADMIN_USER:
            q = q.filter(getattr(cls, 'user_id') == self.user['user_id'])
        q = q.update(_params, synchronize_session=False)
        try:
            db.commit()
        except:
            db.rollback()
        return []

    def local_query_create(self, query):

        assert not query.get_where(), "Filters should be empty for a create request"
        #assert not query.get_select(), "Fields should be empty for a create request"

        cls = self.map_object[query.get_from()]

        params = query.get_params()
        # We encrypt the password according to the encryption of adduser.py
        # As a result from the frontend the new users' password will be inserted
        # into the local DB as encrypted      
        if 'password' in params:
            params['password'] = self.encrypt_password(params['password'])
        
        _params = cls.process_params(query.get_params(), None, self.user)
        new_obj = cls()
        #from sqlalchemy.orm.attributes import manager_of_class
        #mgr = manager_of_class(cls)
        #instance = mgr.new_instance()

        if params:
            for k, v in params.items():
                setattr(new_obj, k, v)
        db.add(new_obj)
        try:
            db.commit()
        except:
            db.rollback()
       
        return [new_obj]

    def local_query_delete(self, query):
        #session.query(User).filter(User.id==7).delete()

        fields = query.fields

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
            if self.user and cls.restrict_to_self and self.user['email'] != ADMIN_USER:
                res = res.filter(cls.user_id == self.user['user_id'])
        except AttributeError: pass

        res.delete()

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
            'get'    : self.local_query_get,
            'update' : self.local_query_update,
            'create' : self.local_query_create,
            'delete' : self.local_query_delete
        }
        table = _map_action[self.query.action](self.query)
        # XXX For local namespace queries, we need to keep a dict
        if table:
            for t in table:
    #MANDO|            row = row2dict(t) if self.format == 'dict' else t.get_object()
                row = row2record(t) if self.format == 'record' else t
                self.send(row)
        self.send(LastRecord())

    def get_metadata(self):
        return []	
