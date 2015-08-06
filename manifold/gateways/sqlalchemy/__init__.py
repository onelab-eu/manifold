#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway which use SQLAlchemy to destination a database (sqlite3, ...)
# http://www.sqlalchemy.org/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

from __future__                         import absolute_import
from types                              import StringTypes
from sqlalchemy                         import types
from sqlalchemy                         import create_engine
from sqlalchemy.ext.declarative         import declarative_base
from sqlalchemy.orm                     import sessionmaker

from manifold.core.announce             import Announce, Announces
from manifold.core.annotation           import Annotation
from manifold.core.destination          import Destination
from manifold.core.field                import Field
from manifold.core.object               import Object
from manifold.core.record               import Records
from manifold.core.table                import Table # XXX isn't it deprecated ?
from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldCollection
from manifold.gateways.sqlalchemy.util  import xgetattr, row2dict
from manifold.util.log                  import Log
from manifold.util.type                 import accepts, returns


# NOTE
# This gateway is synchronous! error management is performed a bit differently
# from async gw
# -- jordan

class SQLAlchemyCollection(ManifoldCollection):

    _map_types = {
        types.Integer : "integer",
        types.Enum    : "integer", # XXX
        types.String  : "string",
        types.Boolean : "bool",
        types.DateTime: "datetime"
    }
    
    def __init__(self, object_name, model = None):
        self._object_name = object_name
        self._model = model

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def get_object(self):
        return Object.from_announce(self.make_announce())

    #---------------------------------------------------------------------
    # Internal usage
    #---------------------------------------------------------------------

    def get_model(self):
        """
        Returns:
            The model class wrapped in this SQLAlchemyCollection
            This is a sqlalchemy.ext.declarative.DeclarativeMeta instance.
        """
        return self._model

    def get_router(self):
        return self.get_gateway().get_router()

    #---------------------------------------------------------------------
    # Methods exposed to the SQLAlchemyGateway Gateway
    #---------------------------------------------------------------------

    @returns(Records)
    def create(self, packet):
        """
        This method must be overloaded if supported in the children class.
        Args:
            packet:
        Returns:
            The list of created Objects.
        """
        destination = packet.get_destination()
        annotation = packet.get_annotation()

        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        if destination.get_filter():
            raise RuntimeError("Filters should be empty for a CREATE Destination (%s): %r" % (destination, destination.get_filter()))

        cls = self.get_model()
        Log.tmp(destination.get_filter())
        Log.tmp(destination.get_field_names())
        params = packet.get_data()
        if "password" in params:
            params["password"] = hash_password(params["password"])

        _params = cls.process_params(params, None, user, self.get_router(), session)
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
            raise RuntimeError, "Destination error: %s" % e
        finally:
            session.rollback()

        rows = [new_obj]
        records = Records([row2record(row) for row in rows])
        self.get_gateway().records(records, packet)

    @returns(Records)
    def update(self, packet):
        """
        This method must be overloaded if supported in the children class.
        Args:
            packet:
        Returns:
            The list of updated Objects.
        """
        destination = packet.get_destination()
        annotation = packet.get_annotation()
    
        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        # XXX The filters that are accepted in config depend on the gateway
        # Real fields : user_credential
        # Convenience fields : credential, then redispatched to real fields
        # same for get, update, etc.

        # XXX What about filters on such fields
        # filter works with account and platform table only
        table_name = destination.get_table_name()
        if table_name == "account" or table_name == "platform":
            if not destination.get_filter().has_eq("platform_id") and not destination.get_filter().has_eq("platform"):
                raise Exception, "Cannot update JSON fields on multiple platforms"

        cls = self.get_model()

        # Note: we can request several values

        # FIELDS: exclude them
        _fields = xgetattr(cls, destination.get_field_names())

        # FILTERS: Note we cannot filter on json fields
        _filters = cls.process_filters(destination.get_filter())
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
        # if there is password update in destination.params
        # We hash the password
        # As a result from the frontend the edited password will be inserted
        # into the local DB as hash
        if "password" in packet.get_data():
            destination.params["password"] = hash_password(destination.params["password"])
        _params = cls.process_params(destination.params, _filters, user, self.get_router(), session)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in destination.params.items() }
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

        self.get_gateway().records(Records(), packet)

    @returns(Records)
    def delete(self, packet):
        """
        This method must be overloaded if supported in the children class.
        Args:
            packet:
        Returns:
            The list of deleted Objects.
        """
        destination = packet.get_destination()
        annotation = packet.get_annotation()

        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        cls = self.get_model()

        # Transform a Filter into a sqlalchemy expression
        #_filters = make_sqla_filters(cls, destination.filters)
        _filters = cls.make_sqla_filters(destination.get_filter())
        _fields = xgetattr(cls, destination.get_field_names()) if destination.get_field_names() else None

        res = session.query(*_fields) if _fields else session.query(cls)
        if destination.filters:
            for _filter in _filters:
                res = res.filter(_filter)

        # Do we need to limit to the user's own results
        try:
            if self.user and cls.restrict_to_self and self.user["email"] != "demo":
                res = res.filter(cls.user_id == self.user["user_id"])
        except AttributeError:
            pass
        res.delete()

        self.get_gateway().records(Records(), packet)

    @returns(Records)
    def get(self, packet):
        """
        Retrieve an Object from the Gateway.
        Args:
            packet:
        Returns:
            A dictionnary containing the requested Gateway object.
        """
        destination = packet.get_destination()
        annotation = packet.get_annotation()

        user = annotation.get("user", None)
        session = self.get_gateway().get_session()

        #
        # XXX How are we handling subqueries
        #

        fields = destination.get_field_names()
        # XXX else tap into metadata

        cls = self.get_model()

        # Transform a Filter into a sqlalchemy expression
        #_filters = make_sqla_filters(cls, destination.get_filter())
        _filters = cls.make_sqla_filters(destination.get_filter())
        _fields = xgetattr(cls, fields) if fields else None

        # db.destination(cls) seems to return NamedTuples
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

        records = [row2dict(row) for row in rows]

        self.get_gateway().records(records, packet)

    @returns(Announce)
    def make_announce(self):
        """
        Returns:
            The list of Announce instances related to this object.
        """
        @returns(StringTypes)
        def camel_to_underscore(string):
            """
            Convert an input camel case string into the corresponding
            underscore string:
            http://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
            Args:
                string: A String instance
            Returns:
                The corresponding underscore string.
            """
            import re
            s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()
            
        table_name = camel_to_underscore(self.__class__.__name__)

        table = Table(None, self._object_name)

        primary_key = tuple()

        for column in self._model.__table__.columns:
            fk = column.foreign_keys
            if fk:
                fk = iter(column.foreign_keys).next()
                _type = str(fk.column.table.name)
            else:
                _type = SQLAlchemyCollection._map_types[column.type.__class__]

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




class SQLAlchemyGateway(Gateway):
    __gateway_name__ = "sqlalchemy"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform_name, **platform_config):
        super(SQLAlchemyGateway, self).__init__(router, platform_name, **platform_config)

        engine = create_engine(platform_config["url"], echo = False)
        from .models.base import Base
        base = declarative_base(cls = Base)
        session = sessionmaker(bind = engine)
        self._session = session()
        base.metadata.create_all(engine)

    def get_session(self):
        """
        Returns:
            A sqlalchemy.orm.session.SessionMaker instance able to
            interact with the database.
        """
        return self._session

    def register_model_collection(self, model, object_name, namespace = None):
        collection = SQLAlchemyCollection(object_name, model)
        self.register_collection(collection, namespace)
