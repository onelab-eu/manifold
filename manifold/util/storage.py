#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class to manage the Manifold Storage which may contains user
# information, platform managed by Manifold and so on. See
# manifold/models for further details. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 


from manifold.core.receiver         import Receiver
from manifold.gateways              import Gateway
from manifold.util.callback         import Callback
from manifold.util.log              import Log 
from manifold.util.type             import accepts, returns

URL = 'sqlite:////var/myslice/db.sqlite?check_same_thread=False'

class Storage(object):
    pass
    # We can read information from files, database, commandline, etc
    # Let's focus on the database

    @classmethod
    def register(self, object):
        """
        Registers a new object that will be stored locally by manifold.
        This will live in the 
        """ 
        pass

class DBStorage(Storage):
    def __init__(self, interface = None, url = URL):
        """
        Constructor.
        Args:
            interface: An Interface instance (the one using this Storage) or None.
            url: A String instance containing the URL of the DBStorage.
                Examples:
                    'sqlite:///:memory:?check_same_thread=False'
                    'sqlite:////var/myslice/db.sqlite?check_same_thread=False'
        """
        # Call SQLAlchemyGateway::__init__() without having a direct
        # dependancy to manifold.gateway.sqlalchemy module.
        storage_config = {"url" : URL}
        self.gateway = Gateway.get("sqlalchemy")(interface, None, storage_config)

    @returns(list)
    def execute(self, query, annotation, receiver):
        """
        Executes a Query on the Manifold Storage and fetches the corresponding results.
        Args:
            query: A Query instance.
            annotation: A dictionnary or None containing Query's annotation.
            receiver: A Receiver instance.
        Returns:
            A list of Record.
        """
        self.gateway.forward(query, annotation, receiver)
