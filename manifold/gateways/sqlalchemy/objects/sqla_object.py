#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Factorize SQLAlchemy object implementation 
#
# Jordan Augé       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from sqlalchemy                         import types
from sqlalchemy.exc                     import IntegrityError 
from sqlalchemy.ext.declarative         import declarative_base

from manifold.core.announce             import Announce
from manifold.core.field                import Field
from manifold.core.record               import Records
from manifold.core.table                import Table
from manifold.gateways.object           import Object
from manifold.gateways.sqlalchemy.util  import xgetattr, row2record
from manifold.util.log                  import Log
from manifold.util.password             import hash_password 
from manifold.util.storage              import STORAGE_NAMESPACE 
from manifold.util.type                 import accepts, returns

class SQLA_Object(Object):

    aliases = dict()

    _map_types = {
        types.Integer : "integer",
        types.Enum    : "integer", # XXX
        types.String  : "string",
        types.Boolean : "bool",
        types.DateTime: "datetime"
    }

    def __init__(self, gateway, model, interface):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            model: A class provided by manifold.models
            interface: A manifold Interface
        """
        super(SQLA_Object, self).__init__(gateway)

        # self.model corresponds to a class inheriting manifold.models.Base
        # and implemented in manifold/models/.
        self.model      = model 
        self._interface = interface

    #---------------------------------------------------------------------
    # Internal usage 
    #---------------------------------------------------------------------

    def get_model(self):
        """
        Returns:
            The model class wrapped in this SQLA_Object 
            This is a sqlalchemy.ext.declarative.DeclarativeMeta instance.
        """
        return self.model

    #---------------------------------------------------------------------
    # Methods exposed to the SQLAlchemyGateway Gateway
    #---------------------------------------------------------------------

    @returns(Records)
    def create(self, query, annotation):
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of created Objects.
        """
        super(SQLA_Object, self).check(query, annotation)
        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        if query.get_where():
            raise RuntimeError("Filters should be empty for a CREATE Query (%s): %r" % (query, query.get_where()))

        cls = self.get_model()

        params = query.get_params()
        if "password" in params:
            params["password"] = hash_password(params["password"])
        
        _params = cls.process_params(params, None, user, self._interface, session)
        new_obj = cls()
        #from sqlalchemy.orm.attributes import manager_of_class
        #mgr = manager_of_class(cls)
        #instance = mgr.new_instance()

        if params:
            for k, v in params.items():
                setattr(new_obj, k, v)
        session.add(new_obj)
        try:
            session.commit()
        except IntegrityError, e:
            raise RuntimeError, "Integrity error: %s" % e
        except Exception, e:
            raise RuntimeError, "Query error: %s" % e
        finally:
            session.rollback()
        
        rows = [new_obj]
        records = Records([row2record(row) for row in rows])
        return records

    @returns(Records)
    def update(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of updated Objects.
        """
        super(SQLA_Object, self).check(query, annotation)
        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        # XXX The filters that are accepted in config depend on the gateway
        # Real fields : user_credential
        # Convenience fields : credential, then redispatched to real fields
        # same for get, update, etc.

        # XXX What about filters on such fields
        # filter works with account and platform table only
        if query.get_from() == "account" or query.get_from() == "platform":
            if not query.get_where().has_eq("platform_id") and not query.get_where().has_eq("platform"):
                raise Exception, "Cannot update JSON fields on multiple platforms"

        cls = self.get_model()

        # Note: we can request several values

        # FIELDS: exclude them
        _fields = xgetattr(cls, query.get_select())

        # FILTERS: Note we cannot filter on json fields
        _filters = cls.process_filters(query.get_where())
        #_filters = make_sqla_filters(cls, _filters)
        _filters = cls.make_sqla_filters(_filters)

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
        if "password" in query.get_params():
            query.params["password"] = hash_password(query.params["password"])
        _params = cls.process_params(query.params, _filters, user, self._interface, session)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in _params.items() ])
       
        #session.query(cls).update(_params, synchronize_session=False)
        q = session.query(cls)
        for _filter in _filters:
            q = q.filter(_filter)

        if user and cls.restrict_to_self:
            q = q.filter(getattr(cls, "user_id") == user["user_id"])

        q = q.update(_params, synchronize_session = False)
        try:
            session.commit()
        except:
            session.rollback()

        return Records()

    @returns(Records)
    def delete(self, query, annotation): 
        """
        This method must be overloaded if supported in the children class.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            The list of deleted Objects.
        """
        super(SQLA_Object, self).check(query, annotation)
        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        cls = self.get_model()

        # Transform a Filter into a sqlalchemy expression
        #_filters = make_sqla_filters(cls, query.filters)
        _filters = cls.make_sqla_filters(query.get_where())
        _fields = xgetattr(cls, query.get_select()) if query.get_select() else None

        res = session.query(*_fields) if _fields else session.query(cls)
        if query.filters:
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if self.user and cls.restrict_to_self and self.user["email"] != "demo":
                res = res.filter(cls.user_id == self.user["user_id"])
        except AttributeError:
            pass
        res.delete()

        return Records()

    @returns(Records)
    def get(self, query, annotation): 
        """
        Retrieve an Object from the Gateway.
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A dictionnary containing the requested Gateway object.
        """
        super(SQLA_Object, self).check(query, annotation)
        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        #
        # XXX How are we handling subqueries
        #

        fields = query.get_select()
        # XXX else tap into metadata

        cls = self.get_model()

        # Transform a Filter into a sqlalchemy expression
        #_filters = make_sqla_filters(cls, query.get_where())
        _filters = cls.make_sqla_filters(query.get_where())
        _fields = xgetattr(cls, fields) if fields else None

        # db.query(cls) seems to return NamedTuples
        res = session.query(*_fields) if _fields else session.query(cls)
        if _filters: 
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if user and cls.restrict_to_self and user["email"] != "demo":
                res = res.filter(cls.user_id == user["user_id"])
        except AttributeError:
            pass

        rows = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in rows]

        return Records([row2record(row) for row in rows])

    @returns(Announce)
    def make_announce(self):
        """
        Returns:
            The list of Announce instances related to this object.
        """
        model = self.get_model()
        table_name = self.__class__.__name__.lower()
        table = Table(STORAGE_NAMESPACE, table_name)

        primary_key = tuple()

        for column in model.__table__.columns:
            fk = column.foreign_keys
            if fk:
                fk = iter(column.foreign_keys).next()
                _type = fk.column.table.name
            else:
                _type = SQLA_Object._map_types[column.type.__class__]

            # We hardcode that certain fields are local
            _qualifiers = list() # ['const', 'local']
            if column.name in ['auth_type', 'config']:
                _qualifiers.append('local')

            # Multiple foreign keys are not handled yet
            table.insert_field(Field(
                name        = column.name,
                type        = _type,
                qualifiers  = _qualifiers, 
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

        return Announce(table)
