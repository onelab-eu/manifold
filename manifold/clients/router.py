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

import traceback
from types                   import StringTypes

from ..clients.client        import ManifoldClient
from ..core.annotation       import Annotation
from ..core.packet           import Packet, GET
from ..core.query            import Query
from ..core.result_value     import ResultValue
from ..core.router           import Router
from ..core.sync_receiver    import SyncReceiver
from ..util.log              import Log
from ..util.type             import accepts, returns

class ManifoldRouterClient(ManifoldClient):

    def __init__(self, user_email = None, storage = None, load_storage = True):
        """
        Constructor.
        Args:
            user_email: A String containing the User's email address.
            storage: A Storage instance or None, set to this Router.
            load_storage: A boolean set to True if the content of this Storage must
                be loaded (storage must be != None).
        """
        assert not user_email or isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))
        assert isinstance(load_storage, bool),\
            "Invalid load_storage = %s (%s)" % (load_storage, type(load_storage))

        super(ManifoldRouterClient, self).__init__()
        self._router = Router()
        #self._router.__enter__()

        if load_storage:
            from manifold.storage import StorageGateway
            storage = StorageGateway(self._router)
            storage.set_up()
        # self.user is a dict or None
        self.init_user(user_email)

    def terminate(self):
        self._router.terminate()

    def get_router(self):
        return self._router

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
        self.user = None

        if not user_email:
            Log.info("Using anonymous profile: %s" % user_email)
            return

        try:
            query_users = Query.get("user").filter_by("email", "==", user_email)
            users = self._router.execute_local_query(query_users)
        except Exception, e:
            Log.warning(traceback.format_exc())
            Log.warning("ManifoldRouterClient::init_user: Cannot initialize user: %s" % e)
            users = list()

        if not len(users) >= 1:
            Log.warning("Cannot retrieve current user (%s)... going anonymous" % user_email)
            self.user = None
        else:
            self.user = users[0]

    #--------------------------------------------------------------
    # Overloaded methods
    #--------------------------------------------------------------

#DEPRECATED|    def __del__(self):
#DEPRECATED|        """
#DEPRECATED|        Shutdown gracefully self._router
#DEPRECATED|        """
#DEPRECATED|        try:
#DEPRECATED|            if self._router:
#DEPRECATED|                self._router.__exit__()
#DEPRECATED|            self._router = None
#DEPRECATED|        except:
#DEPRECATED|            pass

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

    @returns(ResultValue)
    def forward(self, query, annotation = None):
        """
        Send a Query to the nested Manifold Router.
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """
        if not annotation:
            annotation = Annotation()
        annotation |= self.get_annotation()

        receiver = SyncReceiver()

        packet = Packet()
        packet.set_protocol(query.get_protocol())
        data = query.get_data()
        if data:
            packet.set_data(data)


        packet.set_source(self._router.get_address())
        packet.set_destination(query.get_destination())
        packet.update_annotation(self.get_annotation())
        packet.set_receiver(receiver) # Why is it useful ??
        self._router.receive(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value
