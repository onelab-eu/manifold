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

from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.packet           import Packet, QueryPacket
from manifold.core.query            import Query 
from manifold.core.result_value     import ResultValue
from manifold.core.router           import Router
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log 
from manifold.util.type             import accepts, returns
from ..clients.client               import ManifoldClient

class ManifoldLocalClient(ManifoldClient):

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
        #assert isinstance(storage, Storage),\
        #    "Invalid enable_storage = %s (%s)" % (enable_storage, type(enable_storage))
        assert isinstance(load_storage, bool),\
            "Invalid load_storage = %s (%s)" % (load_storage, type(load_storage))

        super(ManifoldLocalClient, self).__init__()
        self.router = Router()
        self.router.__enter__()

        if storage:
            self.router.set_storage(storage)

            if load_storage:
                self.router.load_storage()

        self.init_user(user_email)

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
            Log.info("Using anonymous profile" % user_email)
            return

        if not self.router.has_storage():
            Log.warning("Storage disabled, using anonymous profile instead of '%s' profile" % user_email)
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

    def __del__(self):
        """
        Shutdown gracefully self.router 
        """
        try:
            if self.router:
                self.router.__exit__()
            self.router = None
        except:
            pass

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

    def send(self, packet):
        """
        Send a Packet to the nested Manifold Router.
        Args:
            packet: A QUERY Packet instance.
        """
        assert isinstance(packet, Packet), \
            "Invalid packet %s (%s)" % (packet, type(packet))
        assert packet.get_protocol() == Packet.PROTOCOL_QUERY, \
            "Invalid packet %s of type %s" % (
                packet,
                Packet.get_protocol_name(packet.get_protocol())
            )
        self.router.receive(packet)

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
        packet = QueryPacket(query, annotation, receiver = receiver)
        self.send(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value

