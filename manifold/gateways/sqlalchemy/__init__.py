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

from .objects.account               import Account
from .objects.linked_account        import LinkedAccount
from .objects.platform              import Platform
from .objects.policy                import Policy
from .objects.session               import Session
from .objects.user                  import User

from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

# NOTE
# This gateway is synchronous! error management is performed a bit differently
# from async gw
# -- jordan

class SQLAlchemyGateway(Gateway):
    __gateway_name__ = "sqlalchemy"

    MAP_OBJECT = {
        "platform"       : Platform,
        "user"           : User,
        "account"        : Account,
        "session"        : Session,
        "linked_account" : LinkedAccount,
        "policy"         : Policy
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

        from ..sqlalchemy.models.base import Base
        base = declarative_base(cls = Base)
        session = sessionmaker(bind = engine)
        self._session = session()
        base.metadata.create_all(engine)

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

        # Tables corresponding to a class in manifold.gateways.methods (except
        # sqla_object) (and stored in SQLAlchemy)
        for table_name, cls in self.MAP_OBJECT.items():
            instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self, self._interface)
            announce = instance.make_announce()
            if table_name == "account":
                Log.warning("sqla::__init__(): HACK: adding 'string credential' field in 'account'")
                field = Field(
                    type = "string",
                    name = "credential"
                )
                announce._table.insert_field(field)
            announces.append(announce)

        return announces

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        Log.tmp(query)

        # Since the original query will be altered, we are making a copy here,
        # so that the pit dictionary is not altered
        new_query = query.clone()

        action = query.get_action()
        table_name = query.get_table_name()

# Mando: virtual announces are now in manifold.core.local
#DEPRECATED|        if SQLAlchemyGateway.is_virtual_table(table_name):
#DEPRECATED|            # Handle queries related to local:object and local:gateway.
#DEPRECATED|            # Note that local:column won't be queried since it has no RETRIEVE capability.
#DEPRECATED|            if not action == "get":
#DEPRECATED|                 raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
#DEPRECATED|
#DEPRECATED|            if table_name == "object":
#DEPRECATED|                records = Records([announce.to_dict() for announce in self.get_announces()])
#DEPRECATED|            elif table_name == "gateway":
#DEPRECATED|                records = Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.list().keys())])
#DEPRECATED|            else:
#DEPRECATED|                raise RuntimeError("Invalid table '%s::%s'" % (self.get_platform_name(), table_name))
#DEPRECATED|        else:
#DEPRECATED|            # We need to pass a pointer to the manifold interface to the objects since they have to make # queries
#DEPRECATED|            instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self, self._interface)
#DEPRECATED|            annotation = packet.get_annotation()
#DEPRECATED|            if not annotation:
#DEPRECATED|                annotation = Annotation()
#DEPRECATED|            if not action in ["create", "update", "delete", "get"]:
#DEPRECATED|                raise ValueError("Invalid action = %s" % action)
#DEPRECATED|            records = getattr(instance, action)(new_query, annotation)

        # We need to pass a pointer to the manifold interface to the objects since they have to make # queries
        instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self, self._interface)
        annotation = packet.get_annotation()
        if not annotation:
            annotation = Annotation()
        if not action in ["create", "update", "delete", "get"]:
            raise ValueError("Invalid action = %s" % action)
        records = getattr(instance, action)(new_query, annotation)

        self.records(records, packet)
