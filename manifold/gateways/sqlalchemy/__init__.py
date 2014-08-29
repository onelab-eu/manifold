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

    # MAP_OBJECT maps a SQLAlchemy table name with the corresponding
    # manifold.gateways.sqlalchemy.objects.* Python object.
    # Those Python objects rely with the models defined in
    # manifold.gateways.sqlalchemy.models.*

    # NOTE:
    # Excepted manifold.gateways.sqlalchemy.models.base
    # manifold.gateways.sqlalchemy.{objects,models}.* are specific to
    # the Storage.
    # To keep a SQLAlchemyGateway generic, MAP_OBJECT and the corresponding
    # classes should be defined in manifold.util.storage
    
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

    def __init__(self, router, platform_name, platform_config = None):
        """
        Constructor
        Args:
            router: The Manifold Router on which this Gateway is running.
            platform_name: A String storing name of the platform related to this Gateway.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(SQLAlchemyGateway, self).__init__(router, platform_name, platform_config)

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
            instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self, self._router)
            announce = instance.make_announce()
# MANDO:
# About defining account.credentials:
#
# Since an Account may be related to several credentials (currently hardcoded in account["config"])
# we should have a new table Credential (and maybe one per kind of Credential, like PasswordCredential)
# and perform the JOIN between those tables.
#CRAPPY|            if table_name == "account":
#CRAPPY|                Log.warning("sqla::__init__(): HACK: adding 'string credential' field in 'account'")
#CRAPPY|                field = Field(
#CRAPPY|                    type = "string",
#CRAPPY|                    name = "credential"
#CRAPPY|                )
#CRAPPY|                announce._table.insert_field(field)
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
        instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self, self._router)
        annotation = packet.get_annotation()
        if not annotation:
            annotation = Annotation()
        if not action in ["create", "update", "delete", "get"]:
            raise ValueError("Invalid action = %s" % action)
        records = getattr(instance, action)(new_query, annotation)

        self.records(records, packet)
