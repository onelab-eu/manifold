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

from sqlalchemy                     import create_engine
from sqlalchemy.ext.declarative     import declarative_base
from sqlalchemy.orm                 import sessionmaker

from manifold.core.announce         import Announce, Announces
from manifold.core.annotation       import Annotation
from manifold.core.field            import Field
from manifold.core.record           import Records
from manifold.gateways              import Gateway

from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

# NOTE
# This gateway is synchronous! error management is performed a bit differently
# from async gw
# -- jordan

class SQLAlchemyGateway(Gateway):
    __gateway_name__ = "sqlalchemy"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform_name, platform_config = None):
        """
        Constructor.
        Args:
            router: The Manifold Router on which this Gateway is running.
            platform_name: A String storing name of the platform related to this Gateway.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(SQLAlchemyGateway, self).__init__(router, platform_name, platform_config)

        engine = create_engine(platform_config["url"], echo = False)

        from models.base import Base
        base = declarative_base(cls = Base)
        session = sessionmaker(bind = engine)
        self._session = session()
        base.metadata.create_all(engine)

        # The child class must call set_map_objects otherwise no
        # object can be queried.
        self._map_objects = dict()

    def set_map_objects(self, map_objects):
        """
        Install the mapping between table name and the corresponding
        SQLA_Object. See example in sqla_storage.py.
        Args:
            map_objects: a dict {String : Object} which maps
                a SQLAlchemy table name with the corresponding
                manifold.gateways.sqlalchemy.objects.* Python object.
                Those Python objects rely with the models defined in
                manifold.gateways.sqlalchemy.models.*
        """
        assert isinstance(map_objects, dict)
        self._map_objects = map_objects

    @returns(dict)
    def get_map_objects(self):
        return self._map_objects

    #@returns(SQLA_Object)
    def get_sqla_object(self, table_name):
        cls =  self.get_map_objects()[table_name]
        instance = cls(self, self._router)
        return instance

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def get_session(self):
        """
        Returns:
            A sqlalchemy.orm.session.SessionMaker instance able to
            interact with the database.
        """
        return self._session

    @returns(Announces)
    def make_announces(self):
        """
        Produce Announces corresponding to the table store in
        the SQLAlchemy database wrapped by this SQLAlchemyGateway.
        Returns:
            A list of Announce instances.
        """
        announces = Announces()

        for table_name in self.get_map_objects().keys():
            announce = self.get_sqla_object(table_name).make_announce()
            announces.append(announce)

        return announces

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()

        # Since the original query will be altered, we are making a copy here,
        # so that the pit dictionary is not altered
        new_query = query.clone()

        action = query.get_action()
        table_name = query.get_table_name()

        # We need to pass a pointer to the manifold router to the objects since they have to make # queries
        sqla_object = self.get_sqla_object(table_name)
        annotation = packet.get_annotation()
        if not annotation:
            annotation = Annotation()
        if not action in ["create", "update", "delete", "get"]:
            raise ValueError("Invalid action = %s" % action)
        records = getattr(sqla_object, action)(new_query, annotation)

        self.records(records, packet)
