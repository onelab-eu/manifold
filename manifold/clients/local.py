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
from ..clients.client               import ManifoldClient

class ManifoldLocalClient(ManifoldClient):
    def init_user(self, user_email):
        """
        Initialize self.user.
        Args:
            user_email: A String containing the email address.
        Returns:
            The dictionnary representing this User.
        """
        try:
            users = self.router.execute_local_query(
                Query.get("user").filter_by("email", "==", user_email)
            )
        except:
            users = list()

        if not len(users) >= 1:
            Log.warning("Could not retrieve current user... going anonymous")
            self.user = None
        else:
            self.user = users[0]
#MANDO|            if "config" in self.user and self.user["config"]:
#MANDO|                self.user["config"] = json.loads(self.user["config"])
#MANDO|            else:
#MANDO|                self.user["config"] = None

    def init_router(self):
        """
        Initialize self.router.
        """
        self.router = Router()
        self.router.__enter__()

    def __init__(self, user_email = None):
        """
        Constructor.
        Args:
            user_email: A String containing the User's email address.
        """
        Log.tmp(user_email)
        super(ManifoldLocalClient, self).__init__()
        self.init_user(user_email)

    def del_router(self):
        if self.router:
            self.router.__exit__()
        self.router = None

    def __del__(self):
        """
        Shutdown gracefully the nested Manifold Router.
        """
        try:
            self.del_router()
        except:
            pass

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

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

    #@returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        return self.user


