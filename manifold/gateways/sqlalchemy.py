from __future__                 import absolute_import
from sqlalchemy                 import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm             import sessionmaker
from manifold.gateways          import Gateway
from manifold.models            import *

import traceback
import json

def xgetattr(cls, list_attr):
    ret = []
    for a in list_attr:
        ret.append(getattr(cls,a))
    return tuple(ret)

def get_sqla_filters(cls, filters):
    if filters:
        _filters = None
        for p in filters:
            f = p.op(getattr(cls, p.key), p.value)
            if _filters:
                _filters = _filters and f
            else:
                _filters = f
        return _filters
    else:
        return None

def row2dict(row):
    return {c: getattr(row, c) for c in row.keys()}
    #return {c.name: getattr(row, c.name) for c in row.__table__.columns}

class SQLAlchemyGateway(Gateway):

    map_fact_table = {
        'platform' : Platform,
        'user'     : User,
        'account'  : Account,
        'session'  : Session
    }

    def __init__(self, router=None, platform=None, query=None, config=None, user_config=None, user=None, format='dict'):

        assert format in ['dict', 'object'], 'Unknown return format for gateway SQLAlchemy'
        self.format = format

        super(SQLAlchemyGateway, self).__init__(router, platform, query, config, user_config, user)

        #engine = create_engine('sqlite:///:memory:?check_same_thread=False', echo=False)
        engine = create_engine('sqlite:////var/myslice/db.sqlite?check_same_thread=False', echo=False)
        
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

        cls = self.map_fact_table[query.fact_table]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.fields) if query.fields else None

        res = db.query( *_fields ) if _fields else db.query( cls )
        if query.filters:
            res = res.filter(_filters)
        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]

        return tuplelist

    def local_query_update(self, query):

        # XXX The filters that are accepted in config depend on the gateway
        # Real fields : user_credential
        # Convenience fields : credential, then redispatched to real fields
        # same for get, update, etc.

        # XXX What about filters on such fields

        try:
            if not query.filters.has_eq('platform_id') and not query.filters.has_eq('platform'):
                raise Exception, "Cannot update JSON fields on multiple platforms"

            cls = self.map_fact_table[query.fact_table]

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
            _params = cls.process_params(query.params, _filters, self.user)
            # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
            _params = dict([ (getattr(cls, k), v) for k,v in _params.items() ])

            #db.query(cls).update(_params, synchronize_session=False)
            q = db.query(cls).filter(_filters)
            if cls.user_filter:
                q = q.filter(getattr(cls, 'user_id') == self.user.user_id)
            q = q.update(_params, synchronize_session=False)
            db.commit()
        except Exception, e:
            print "Exception in local query update", e
            print traceback.print_exc()

        return []

    def local_query_create(self, query):

        assert not query.filters, "Filters should be empty for a create request"
        #assert not query.fields, "Fields should be empty for a create request"

        cls = self.map_fact_table[query.fact_table]
        params = cls.process_params(query.params, None, self.user)
        new_obj = cls(**params)
        print "local_query_create ---- new_obj = ",new_obj
        db.add(new_obj)
        db.commit()
        
        return []

    def start(self):
        assert self.query, "Cannot start gateway with no query associated"
        _map_action = {
            "get"    : self.local_query_get,
            "update" : self.local_query_update,
            "create" : self.local_query_create
        }
        print "sqlalchemy ---- query = ",self.query
        table = _map_action[self.query.action](self.query)
        # XXX For local namespace queries, we need to keep a dict
        for t in table:
            row = row2dict(t) if self.format == 'dict' else t.get_object()
            self.callback(row)
        self.callback(None)

    def get_metadata(self):
        return []
