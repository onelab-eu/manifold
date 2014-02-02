#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldLocalClient is used to perform query on
# a Manifold Router that we run locally. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.annotation       import Annotation
from manifold.core.query            import Query 
from manifold.core.router           import Router
from manifold.util.log              import Log 
from manifold.util.type             import accepts, returns
from manifold.util.storage          import SQLAlchemyStorage
from ..clients.client               import ManifoldClient

class ManifoldLocalClient(ManifoldClient):

    def __init__(self, user_email = None):
        """
        Constructor.
        Args:
            user_email: A String containing the User's email address.
        """
        super(ManifoldLocalClient, self).__init__()

        storage = SQLAlchemyStorage(
            platform_config = None,
            interface       = self.router 
        )
        self.router.set_storage(storage)
        self.router.load_storage()

        self.init_user(user_email)
        self.router.__enter__()

    #--------------------------------------------------------------
    # Internal methods 
    #--------------------------------------------------------------

    def init_user(self, user_email):
        """
        Initialize self.user.
        Args:
            user_email: A String containing the email address.
        Returns:
            The dictionnary representing this User.
        """
        if not self.router.has_storage():
            Log.warning("Storage disabled, using anonymous profile instead of '%s' profile" % user_email)
            self.user = None
            return

        try:
            users = self.router.execute_local_query(
                Query.get("user").filter_by("email", "==", user_email)
            )
        except:
            users = list()

        if not len(users) >= 1:
            Log.warning("Cannot retrieve current user (%s)... going anonymous" % user_email)
            self.user = None
        else:
            self.user = users[0]
#MANDO|            if "config" in self.user and self.user["config"]:
#MANDO|                self.user["config"] = json.loads(self.user["config"])
#MANDO|            else:
#MANDO|                self.user["config"] = None

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

    def del_router(self):
        """
        Shutdown gracefully self.router 
        """
        if self.router:
            self.router.__exit__()

    def make_router(self):
        """
        Initialize self.router.
        """
        router = Router()
        return router

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation to pass to the QUERY Packet
            sent to the Router.
        """
        return Annotation({"user" : self.user}) 
    
    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        """
        if self.user:
            return "Shell using local account %s" % self.user["email"]
        else:
            return "Shell using no account"

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        return self.user


