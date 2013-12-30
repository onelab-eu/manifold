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

import traceback

from sqlalchemy                     import exc
from sqlalchemy                     import create_engine
from sqlalchemy                     import types
from sqlalchemy.ext.declarative     import declarative_base
from sqlalchemy.orm                 import sessionmaker

try:
    from sqlalchemy.util._collections   import NamedTuple
except ImportError:
    # NamedTuple was renamed in latest sqlalchemy versions
    from sqlalchemy.util._collections   import KeyedTuple as NamedTuple

from manifold.core.announce         import Announce
from manifold.core.annotation       import Annotation
from manifold.core.field            import Field
from manifold.core.record           import Record, Records, LastRecord 
from manifold.core.table            import Table
from manifold.gateways              import Gateway
#from manifold.models                import db
from manifold.models.account        import Account
from manifold.models.linked_account import LinkedAccount
from manifold.models.platform       import Platform
from manifold.models.policy         import Policy
from manifold.models.session        import Session as DBSession 
from manifold.models.user           import User
from manifold.util.log              import Log
from manifold.util.password         import hash_password 
from manifold.util.predicate        import included
from manifold.util.storage          import get_metadata_tables, STORAGE_NAMESPACE 
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
    for a in list_attr:
        ret.append(getattr(cls, a))
    return tuple(ret)

@returns(list)
def get_sqla_filters(cls, predicates):
    """
    Convert a list of Predicate instances (i.e. WHERE Clause provided
    by Query) into a list of sqlalchemy filters.
    Args:
        predicates: A list of Predicate instances or None.
    Returns:
        The corresponding filters (or None if predicates == None).
    """
    if predicates:
        _filters = list() 
        for predicate in predicates:
            if predicate.get_op() == included:
                f = getattr(cls, predicate.get_key()).in_(predicate.get_value())
            else:
                f = predicate.get_op()(
                    getattr(cls, predicate.get_key()),
                    predicate.get_value()
                )
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
    # http://stackoverflow.com/questions/18110033/getting-first-row-from-sqlalchemy
    # When you ask specifically for a column of a mapped class with
    # query(Class.attr), SQLAlchemy will return a
    # sqlalchemy.util._collections.NamedTuple instead of DB objects.

    if isinstance(row, NamedTuple):
        return Record(zip(row.keys(), row))
    else:
        return Record({c.name: getattr(row, c.name) for c in row.__table__.columns})

class SQLAlchemyGateway(Gateway):
    __gateway_name__ = "sqlalchemy"

    _map_object = {
        "platform"       : Platform,
        "user"           : User,
        "account"        : Account,
        "session"        : DBSession,
        "linked_account" : LinkedAccount,
        "policy"         : Policy
    }

    _map_types = {
        types.Integer : "integer",
        types.Enum    : "integer", # XXX
        types.String  : "string",
        types.Boolean : "bool"
    }

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, interface, platform_name, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the platform related to this Gateway.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(SQLAlchemyGateway, self).__init__(interface, platform_name, platform_config)

        engine = create_engine(platform_config["url"], echo = False)

        from manifold.models.base import Base
        Base = declarative_base(cls = Base)
        Session = sessionmaker(bind = engine)
        self.db = Session()
        
        from manifold.models.platform       import Platform
        from manifold.models.user           import User
        from manifold.models.account        import Account
        from manifold.models.session        import Session
        from manifold.models.linked_account import LinkedAccount
        from manifold.models.policy         import Policy
        Base.metadata.create_all(engine)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------
        
    @returns(list)
    def local_query_get(self, query, user):
        """
        Perform a SELECT ... FROM ... query on the sqlalchemy database
        wrapped by this SQLAlchemyGateway.
        Args:
            query: A Query instance.
            user: A dictionnary representing the User issuing the Query
                or None (anonymous access).
        Returns:
            The list of object (see manifold/models) resulting from
            this Query.
        """
        #
        # XXX How are we handling subqueries
        #

        fields = query.get_select()
        # XXX else tap into metadata

        cls = self._map_object[query.get_from()]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.get_where())
        _fields = xgetattr(cls, fields) if fields else None

        # db.query(cls) seems to return NamedTuples
        res = self.db.query(*_fields) if _fields else self.db.query(cls)
        if _filters: 
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
        """
        Perform a UPDATE ... query on the sqlalchemy database
        wrapped by this SQLAlchemyGateway.
        Args:
            query: A Query instance.
            user: A dictionnary representing the User issuing the Query
                or None (anonymous access).
        Returns:
            An empty list.
        """
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
       
        #self.db.query(cls).update(_params, synchronize_session=False)
        q = self.db.query(cls)
        for _filter in _filters:
            q = q.filter(_filter)

        if user and cls.restrict_to_self:
            q = q.filter(getattr(cls, 'user_id') == user['user_id'])

        q = q.update(_params, synchronize_session = False)
        try:
            self.db.commit()
        except:
            self.db.rollback()

        return list() 

    @returns(list)
    def local_query_create(self, query, user):
        """
        Perform a INSERT ... INTO query on the sqlalchemy database
        wrapped by this SQLAlchemyGateway.
        Args:
            query: A Query instance.
            user: A dictionnary representing the User issuing the Query
                or None (anonymous access).
        Returns:
            The list of inserted object.
        """
        assert not query.get_where(), "Filters should be empty for a CREATE Query (%s)" % query

        cls = self._map_object[query.get_from()]

        params = query.get_params()
        if 'password' in params:
            params['password'] = hash_password(params['password'])
        
        _params = cls.process_params(params, None, user)
        new_obj = cls()
        #from sqlalchemy.orm.attributes import manager_of_class
        #mgr = manager_of_class(cls)
        #instance = mgr.new_instance()

        if params:
            for k, v in params.items():
                setattr(new_obj, k, v)
        self.db.add(new_obj)
        try:
            self.db.commit()
        except exc.IntegrityError, e:
            raise Exception, "Integrity error: %e" % e
        except Exception, e:
            raise Exception, "Query error: %e" % e
        finally:
            self.db.rollback()
        
        return [new_obj]

    def local_query_delete(self, query):
        """
        Perform a DELETE ... FROM query on the sqlalchemy database
        wrapped by this SQLAlchemyGateway.
        Args:
            query: A Query instance.
        """
        cls = self._map_object[query.get_from()]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.get_select()) if query.get_select() else None

        res = self.db.query(*_fields) if _fields else self.db.query(cls)
        if query.filters:
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if self.user and cls.restrict_to_self and self.user['email'] != 'demo':
                res = res.filter(cls.user_id == self.user['user_id'])
        except AttributeError: pass
        res.delete()

    @returns(list)
    def make_announces(self):
        """
        Produce Announces corresponding to the table store in
        the SQLAlchemy database wrapped by this SQLAlchemyGateway.
        Returns:
            A list of Announce instances.
        """
        announces = list()

        # Each model is a table ("account", "linked_account", "user"...)
        for table_name, cls in self._map_object.items():
            table = Table(STORAGE_NAMESPACE, None, table_name, None, None)

            primary_key = tuple()

            for column in cls.__table__.columns:
                table.insert_field(Field(
                    qualifiers  = list(), # nothing ["const"]
                    name        = column.name,
                    type        = self._map_types[column.type.__class__],
                    is_array    = False,
                    description = column.description
                ))

                if column.primary_key:
                    primary_key += (column.name, )
                
            table.insert_key(primary_key)
        
            isnt_table_object = (table_name == "object")
            table.capabilities.retrieve   = isnt_table_object
            table.capabilities.join       = isnt_table_object
            table.capabilities.selection  = isnt_table_object
            table.capabilities.projection = isnt_table_object

            announces.append(Announce(table))

        # Meta-tables "object" and "column"
        metadata_announces = get_metadata_tables(STORAGE_NAMESPACE)
        announces.extend(metadata_announces)
        return announces

    def receive(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_receive(packet)
        query = packet.get_query()
        #socket = self.get_socket(query)

        _map_action = {
            "get"    : self.local_query_get,
            "update" : self.local_query_update,
            "create" : self.local_query_create,
            "delete" : self.local_query_delete
        }

        try:
            if query.get_from() == "object":
                if not query.get_action() == "get":
                    raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (query.get_action(), self.get_platform_name(), table_name))

                records = Records([announce.to_dict() for announce in self.get_announces()])
            else:
                annotation = packet.get_annotation()
                if not annotation:
                    annotation = Annotation() 
                user = annotation.get("user", None)
                records = Records([row2record(row) for row in _map_action[query.get_action()](query, user)])

            self.records(query, records)
        except Exception, e:
            #Log.error(traceback.format_exc())
            self.error(query, 0, 0, e)
        finally:
            self.close(query)
