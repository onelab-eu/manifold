#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class to manage Manifold's gateways
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

import sys

from manifold.core.record               import Record, LastRecord
from manifold.gateways                  import Gateway
from manifold.util.log                  import Log

class ManifoldGateway(Gateway):
    __gateway_name__ = 'manifold'

    def __str__(self):
        """
        Returns:
            The '%s' representation of this ManifoldGateway.
        """
        return "<ManifoldGateway %s %s>" % (self.config["url"], self.query)

    def success_cb(self, records, callback, identifier):
        """
        Args:
            records: The list containing the fetched Records.
        """
        for record in records:
            self.send(Record(record), callback, identifier)
        self.send(LastRecord(), callback, identifier)
        self.success(receiver, query)

    def exception_cb(self, error, callback, identifier):
        Log.warning("Error during Manifold call: %s" % error)
        self.send(LastRecord(), callback, identifier)
        # XXX self.error(receiver, query)

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, account_config = None, format = "dict", receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean set to True if this Query is async.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        Gateway.forward(self, query, callback, is_deferred, execute, user, account_config, format, receiver)
        identifier = receiver.get_identifier() if receiver else None

        from twisted.web.xmlrpc import Proxy
        try:
            def wrap(source):
                proxy = Proxy(self.config['url'], allowNone = True)
                query = source.query
                auth = {'AuthMethod': 'guest'}

                # DEBUG
                if self.config['url'] == "https://api2.top-hat.info/API/":
                    Log.warning("Hardcoding XML RPC call")

                    # Where conversion
                    filters = {}
                    for predicate in query.filters:
                        field = "%s%s" % ('' if predicate.get_str_op() == "=" else predicate.op, predicate.key)
                        if field not in filters:
                            filters[field] = []
                        filters[field].append(predicate.value)
                    for field, value in filters.iteritems():
                        if len(value) == 1:
                            filters[field] = value[0]
                    query.filters = filters

                Log.info("Issuing xmlrpc call to %s: %s" % (self.config['url'], query))

                proxy.callRemote(
                    'Get',
                    auth,
                    query.get_from(),
                    query.get_timestamp(),
                    query.get_where(),
                    list(query.get_select())
                ).addCallbacks(source.success_cb, source.exception_cb, callback, identifier)

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
            wrap(self)
            
        except Exception, e:
            Log.warning("Exception in ManifoldGateway::start()", e)
