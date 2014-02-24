#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# Lease object exposed by a AM.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from resource_lease import ResourceLease
from twisted.internet                   import defer
from manifold.gateways.deferred_object  import DeferredObject 

class Resource(ResourceLease):

    @defer.inlineCallbacks
    def get(self, user, user_account_config, query):
        rsrc_lease = yield ResourceLease(self.get_gateway()).get(user, user_account_config, query)
        resources = rsrc_lease['resource']
        defer.returnValue(resources)
