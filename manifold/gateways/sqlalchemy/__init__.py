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

from __future__                     import absolute_import

import json, sys, time, traceback

from hashlib                        import md5
from random                         import Random

from sqlalchemy                     import create_engine
from sqlalchemy                     import types
from sqlalchemy.ext.declarative     import declarative_base, declared_attr
from sqlalchemy.orm                 import sessionmaker
from sqlalchemy.util._collections   import NamedTuple ##
from manifold.core.announce         import Announce
from manifold.core.field            import Field
from manifold.core.query            import Query ##
from manifold.core.record           import Record, LastRecord 
from manifold.core.table            import Table
from manifold.gateways              import Gateway
from manifold.models                import db
from manifold.models.account        import Account
from manifold.models.linked_account import LinkedAccount
from manifold.models.platform       import Platform
from manifold.models.policy         import Policy
from manifold.models.session        import Session as DBSession 
from manifold.models.user           import User
from manifold.util.log              import Log
from manifold.util.password         import hash_password 
from manifold.util.predicate        import included
from manifold.util.type             import accepts, returns 

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

@returns(list)
def get_sqla_filters(cls, filters):
    if filters:
        _filters = list() 
        for p in filters:
            if p.op == included:
                f = getattr(cls, p.key).in_(p.value)
            else:
                f = p.op(getattr(cls, p.key), p.value)
            _filters.append(f)
        return _filters
    else:
        return None

@returns(Record)
def row2record(row):
    """
    Convert a python object into the corresponding dictionnary, based
    on its attributes.
    Args:
        row: A instance based on a type which is
            either in manifold/models
            or either sqlalchemy.util._collections.NamedTuple
    Returns:
        The corresponding Record.
    """
    try:
        return Record({c.name: getattr(row, c.name) for c in row.__table__.columns})
    except:
        Log.warning("row2record: this is strange: row = %s (%s)" % (row, type(row)))
        Log.warning(traceback.format_exc())
        pass

    if isinstance(row, NamedTuple):
        return Record(zip(row.keys(), row))

class SQLAlchemyGateway(Gateway):
    __gateway_name__ = 'sqlalchemy'

    _map_object = {
        "platform"       : Platform,
        "user"           : User,
        "account"        : Account,
        "session"        : DBSession,
        "linked_account" : LinkedAccount,
        "policy"         : Policy
    }

    _map_types = {
        types.Integer: 'integer',
        types.Enum:    'integer', # XXX
        types.String:  'string',
        types.Boolean: 'bool'
    }

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

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
        Base = declarative_base(cls = Base)
        
        # Models
        from manifold.models.account        import Account       as DBAccount
        from manifold.models.linked_account import LinkedAccount as DBLinkedAccount
        from manifold.models.platform       import Platform      as DBPlatform
        from manifold.models.policy         import Policy        as DBPolicy
        from manifold.models.session        import Session       as DBSession
        from manifold.models.user           import User          as DBUser

        engine = create_engine(config['url'], echo = False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind = engine)
        self.db = Session()

        self._metadata = None


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------
        
    @returns(list)
    def local_query_get(self, query, user):
        #
        # XXX How are we handling subqueries
        #

        fields = query.get_select()
        # XXX else tap into metadata

        cls = self._map_object[query.get_from()]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.get_where())
        _fields = xgetattr(cls, query.get_select()) if query.get_select() else None

        res = db.query(*_fields) if _fields else db.query(cls)
        if query.get_where():
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if user and cls.restrict_to_self and user['email'] != 'demo':
                res = res.filter(cls.user_id == user['user_id'])
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

        cls = self._map_object[query.get_from()]

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
        # We hash the password
        # As a result from the frontend the edited password will be inserted
        # into the local DB as hash 
        if 'password' in query.get_params():
            query.params['password'] = hash_password(query.params['password'])
        _params = cls.process_params(query.params, _filters, user)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in _params.items() ])
       
        #db.query(cls).update(_params, synchronize_session=False)
        q = db.query(cls)
        for _filter in _filters:
            q = q.filter(_filter)

        if user and cls.restrict_to_self:
            q = q.filter(getattr(cls, 'user_id') == user['user_id'])

        q = q.update(_params, synchronize_session=False)
        try:
            db.commit()
        except:
            db.rollback()

        return list() 

    @returns(list)
    def local_query_create(self, query, user):
        assert not query.get_where(), "Filters should be empty for a create request"

        cls = self._map_object[query.get_from()]

        params = query.get_params()
        if 'password' in params:
            params['password'] = hash_password(params['password'])
        
        _params = cls.process_params(query.get_params(), None, user)
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

        cls = self._map_object[query.object]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.fields) if query.fields else None


        res = db.query( *_fields ) if _fields else db.query( cls )
        if query.filters:
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if self.user and cls.restrict_to_self and self.user['email'] != 'demo':
                res = res.filter(cls.user_id == self.user['user_id'])
        except AttributeError: pass

        res.delete()

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

    def forward(self, query, annotation, callback, is_deferred = False, execute = True, account_config = None, receiver = None):
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
            account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        super(SQLAlchemyGateway, self).forward(query, annotation, callback, is_deferred, execute, account_config, receiver)
        identifier = receiver.get_identifier() if receiver else None

        assert isinstance(query, Query), "Invalid query"
        _map_action = {
            'get'    : self.local_query_get,
            'update' : self.local_query_update,
            'create' : self.local_query_create,
            'delete' : self.local_query_delete
        }

        try:

            if query.get_from() == 'object':
                if not query.get_action() == 'get':
                    raise Exception, "Invalid query on local object table"
                return self.get_metadata()

            user = annotation.get('user', None)
            rows = _map_action[query.get_action()](query, user)
            for row in rows:
                self.send(row2record(row), callback, identifier)
            self.send(LastRecord(), callback, identifier)
            self.success(receiver, query)
        except AttributeError, e:
            self.send(LastRecord(), callback, identifier)
            self.error(receiver, query, e)

    @returns(list)
    def make_metadata(self):
        announces = list()

        # Each model is a table
        for table_name, cls in self._map_object.items():
            table = Table('local', None, table_name, None, None)

            primary_key = tuple()

            for column in cls.__table__.columns:
                table.insert_field(Field(
                    qualifiers  = [], # nothing ["const"]
                    name        = column.name,
                    type        = self._map_types[column.type.__class__],
                    is_array    = False,
                    description = column.description
                ))

                if column.primary_key:
                    primary_key += (column.name, )
                
            table.insert_key(primary_key)
        
            table.capabilities.retrieve   = True
            table.capabilities.join       = True
            table.capabilities.selection  = True
            table.capabilities.projection = True
                
            announces.append(table)

        metadata_announces = Announce.get_metadata_tables('local')
        announces.extend([a.table for a in metadata_announces])
        return announces

    # XXX This might be factored ?
    @returns(list)
    def get_metadata(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        if not self._metadata:
            self._metadata = self.make_metadata()
        return self._metadata

    def receive(self, packet):
        # formerly forward
        Gateway.receive(self, packet)

        _map_action = {
            'get'    : self.local_query_get,
            'update' : self.local_query_update,
            'create' : self.local_query_create,
            'delete' : self.local_query_delete
        }

        try:
            query = packet.get_query()
            annotation = packet.get_annotation()

            if query.get_from() == 'object':
                if not query.get_action() == 'get':
                    raise Exception, "Invalid query on local object table"
                for record in self.get_metadata():
                    self.send(Record(record))

            user = annotation.get('user', None)
            rows = _map_action[query.get_action()](query, user)
            print "sending records"
            for row in rows:
                self.send(row2record(row)) #, callback, identifier)
            print "sending last"
            self.send(LastRecord())
        except AttributeError, e:
            self.send(LastRecord())
            self.error(query, e)
