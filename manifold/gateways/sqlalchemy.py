from __future__ import absolute_import
from manifold.gateways      import Gateway

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

from manifold.models import *

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
                _filters = f and _filters
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
        'platform' : DBPlatform,
        'user'     : DBUser,
        'account'  : DBAccount,
        'session'  : DBSession
    }

    def __init__(self, format='dict'):

        assert format in ['dict', 'object'], 'Unknown return format for gateway SQLAlchemy'
        self.format = format

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
        
            @staticmethod
            def process_params(params):
                return params
        
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
        if _filters:
            res = res.filter(_filters)

        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]

        return tuplelist

    def local_query_update(self, query):

        cls = self.map_fact_table[query.fact_table]

        _fields = xgetattr(cls, query.fields)
        _filters = get_sqla_filters(cls, query.filters)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in query.params.items() ])

        #db.query(cls).update(_params, synchronize_session=False)
        db.query(cls).filter(_filters).update(_params, synchronize_session=False)
        db.commit()

        return []

    def local_query_create(self, query):

        assert not query.filters, "Filters should be empty for a create request"
        #assert not query.fields, "Fields should be empty for a create request"


        cls = self.map_fact_table[query.fact_table]
        params = cls.process_params(query.params)
        new_obj = cls(**params)
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
        table = _map_action[self.query.action](self.query)
        # XXX For local namespace queries, we need to keep a dict
        for t in table:
            row = row2dict(t) if self.format == 'dict' else t.get_object()
            self.callback(row)
        self.callback(None)

    def get_metadata(self):
        return []
