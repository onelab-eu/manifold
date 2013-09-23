#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway which use SQLAlchemy to query a database (sqlite3, ...)
# http://www.sqlalchemy.org/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from __future__                 import absolute_import

import crypt, json, sys, time, traceback

from hashlib                    import md5
from random                     import Random
from types                      import StringTypes

from sqlalchemy                 import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm             import sessionmaker
from manifold.core.query        import Query
from manifold.gateways.gateway  import Gateway
from manifold.models            import db
from manifold.models.account    import Account
from manifold.models.platform   import Platform
from manifold.models.user       import User
from manifold.models.session    import Session as DBSession 
from manifold.operators         import LAST_RECORD
from manifold.util.log          import Log
from manifold.util.predicate    import included
from manifold.util.type         import accepts, returns 

@returns(tuple)
def xgetattr(cls, list_attr):
    """
    Extract attributes from an instance to make the corresponding tuple.
    Args:
        list_attr: A list of Strings corresponding to attribute
            names of cls that must be retrieved.
    Returns:
        The corresponding tuple.
    """
    ret = list()
    invalid_attributes = list()
    for a in list_attr:
        ret.append(getattr(cls, a))
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

@returns(dict)
def row2dict(row):
    """
    Convert a python object into the corresponding dictionnary, based
    on its attributes.
    Args:
        row:
    Returns:
        The corresponding dictionnary.
    """
    try:
        return {c.name: getattr(row, c.name) for c in row.__table__.columns}
    except:
        for field in row.__table__.columns:
            try:
                _ =  getattr(row, field.name)
            except:
                break
        Log.tmp("Inconsistency in ROW2DICT: expected columns: %s ; this one (%s) is not in %s" %
            (
                row.__table__.columns,
                field
            )
        )
        return {c: getattr(row, c) for c in row.keys()}

class SQLAlchemyGateway(Gateway):

    map_object = {
        'platform' : Platform,
        'user'     : User,
        'account'  : Account,
        'session'  : DBSession
    }

    def __init__(self, interface, platform, config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway.
            config: A dictionnary containing the configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        super(SQLAlchemyGateway, self).__init__(interface, platform, config)

        from manifold.models.base import Base
        Base = declarative_base(cls=Base)
        
        # Models
        from manifold.models.platform   import Platform as DBPlatform 
        from manifold.models.user       import User     as DBUser
        from manifold.models.account    import Account  as DBAccount
        from manifold.models.session    import Session  as DBSession

        engine = create_engine(config['url'], echo = False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind = engine)
        self.db = Session()
        
    @returns(list)
    def local_query_get(self, query, user):
        #
        # XXX How are we handling subqueries
        #

        fields = query.get_select()
        # XXX else tap into metadata

        cls = self.map_object[query.get_from()]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.get_where())
        _fields = xgetattr(cls, query.get_select()) if query.get_select() else None

        res = db.query(*_fields) if _fields else db.query(cls)
        if query.get_where():
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if cls.restrict_to_self and user.email != 'demo':
                res = res.filter(cls.user_id == user.user_id)
        except AttributeError: pass

        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]

        return tuplelist

    @returns(list)
    def local_query_update(self, query, user):

        # XXX The filters that are accepted in config depend on the gateway
        # Real fields : user_credential
        # Convenience fields : credential, then redispatched to real fields
        # same for get, update, etc.

        # XXX What about filters on such fields
        # filter works with account and platform table only
        if query.get_from() == 'account' or query.get_from() == 'platform':
            if not query.get_where().has_eq('platform_id') and not query.get_where().has_eq('platform'):
                raise Exception, "Cannot update JSON fields on multiple platforms"

        cls = self.map_object[query.get_from()]

        # Note: we can request several values

        # FIELDS: exclude them
        _fields = xgetattr(cls, query.get_select())


        # FILTERS: Note we cannot filter on json fields
        _filters = cls.process_filters(query.get_where())
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
            query.params['password'] = SQLAlchemyGateway.encrypt_password(query.params['password'])
        _params = cls.process_params(query.params, _filters, user)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in _params.items() ])
       
        #db.query(cls).update(_params, synchronize_session=False)
        q = db.query(cls)
        for _filter in _filters:
            q = q.filter(_filter)
        if cls.restrict_to_self:
            q = q.filter(getattr(cls, 'user_id') == user.user_id)
        q = q.update(_params, synchronize_session=False)
        db.commit()

        return []

    @returns(list)
    def local_query_create(self, query, user):
        assert not query.get_where(), "Filters should be empty for a create request"

        cls = self.map_object[query.get_from()]

        params = query.get_params()
        # We encrypt the password according to the encryption of adduser.py
        # As a result from the frontend the new users' password will be inserted
        # into the local DB as encrypted      
        if 'password' in params:
            params['password'] = SQLAlchemyGateway.encrypt_password(params['password'])
        
        _params = cls.process_params(query.get_params(), None, self.user)
        new_obj = cls()
        #from sqlalchemy.orm.attributes import manager_of_class
        #mgr = manager_of_class(cls)
        #instance = mgr.new_instance()

        if params:
            for k, v in params.items():
                print "%s = %s" % (k,v)
                setattr(new_obj, k, v)
        self.db.add(new_obj)
        self.db.commit()
        
        return [new_obj]

    @staticmethod
    @returns(StringTypes)
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

#MANDO|    def start(self):
#MANDO|        assert self.query, "Cannot start gateway with no query associated"
#MANDO|        _map_action = {
#MANDO|            "get"    : self.local_query_get,
#MANDO|            "update" : self.local_query_update,
#MANDO|            "create" : self.local_query_create
#MANDO|        }
#MANDO|        table = _map_action[self.query.action](self.query)
#MANDO|        # XXX For local namespace queries, we need to keep a dict
#MANDO|        for t in table:
#MANDO|            row = row2dict(t) if self.format == 'dict' else t.get_object()
#MANDO|            row = row2dict(t) if self.format == 'dict' else t
#MANDO|            self.callback(row)
#MANDO|        self.callback(None)

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, format = "dict", from_node = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            format: A String specifying in which format the Records must be returned.
            from_node : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        self.check_forward(query, is_deferred, execute, user, from_node)
        identifier = from_node.get_identifier() if from_node else None

        assert isinstance(query, Query), "Invalid query"
        _map_action = {
            "get"    : self.local_query_get,
            "update" : self.local_query_update,
            "create" : self.local_query_create
        }

        try:
            rows = _map_action[query.get_action()](query, user)
            for row in rows:
                self.send(row2dict(row) if format == "dict" else row, callback, identifier)
            self.send(LAST_RECORD, callback, identifier)
            self.success(from_node, query)
        except AttributeError, e:
            self.send(LAST_RECORD, callback, identifier)
            self.error(from_node, query, e)

    @returns(list)
    def get_metadata(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        return list()
