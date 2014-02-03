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

from manifold.core.announce         import Announce, make_virtual_announces
from manifold.core.annotation       import Annotation
from manifold.core.record           import Records
from manifold.gateways              import Gateway

from manifold.gateways.sqlalchemy.methods.account        import Account
from manifold.gateways.sqlalchemy.methods.linked_account import LinkedAccount
from manifold.gateways.sqlalchemy.methods.platform       import Platform
from manifold.gateways.sqlalchemy.methods.policy         import Policy
from manifold.gateways.sqlalchemy.methods.session        import Session
from manifold.gateways.sqlalchemy.methods.user           import User

from manifold.util.log              import Log
from manifold.util.type             import accepts, returns
from manifold.util.storage          import STORAGE_NAMESPACE 

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

    @staticmethod
    @returns(bool)
    def is_virtual_table(table_name):
        """
        Tests whether a Table is virtual or not. A Table is said to be
        the Manifold object do not rely to a SQLAlchemy table.
        Args:
            table_name: A String instance.
        Returns:
            True iif this Table is virtual.
        """
        virtual_table_names = [announce.get_table().get_name() for announce in make_virtual_announces(STORAGE_NAMESPACE)]
        return table_name in virtual_table_names 

    @returns(list)
    def make_announces(self):
        """
        Produce Announces corresponding to the table store in
        the SQLAlchemy database wrapped by this SQLAlchemyGateway.
        Returns:
            A list of Announce instances.
        """
        announces = list()

        # Tables corresponding to a class in manifold.gateways.methods (except
        # sqla_object) (and stored in SQLAlchemy)
        for table_name, cls in self.MAP_OBJECT.items():
            instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self)
            announces.append(instance.make_announce())

        # Virtual tables ("object", "column", ...) 
        virtual_announces = make_virtual_announces(STORAGE_NAMESPACE)
        announces.extend(virtual_announces)

        return announces

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        action = query.get_action()
        table_name = query.get_from()

        if SQLAlchemyGateway.is_virtual_table(table_name):
            # Handle queries related to local:object and local:gateway.
            # Note that local:column won't be queried since it has no RETRIEVE capability. 
            if not action == "get":
                 raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
 
            if table_name == "object":            
                records = Records([announce.to_dict() for announce in self.get_announces()])
            elif table_name == "gateway":
                records = Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.list().keys())])
            else:
                raise RuntimeError("Invalid table '%s::%s'" % (self.get_platform_name(), table_name))
        else:
            instance = SQLAlchemyGateway.MAP_OBJECT[table_name](self)
            annotation = packet.get_annotation()
            if not annotation:
                annotation = Annotation()
            if not action in ["create", "update", "delete", "get"]:
                raise ValueError("Invalid action = %s" % action)
            records = getattr(instance, action)(query, annotation)

        self.records(packet, records)
