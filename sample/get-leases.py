#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from manifold.core.router import THLocalRouter
from manifold.core.router import Query

query = ('get', 'slice', [['slice_hrn', '=', 'ple.upmc.agent']], {}, ['slice_hrn', 'resource.hostname'])

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate({'AuthMethod': 'password', 'Username': 'jordan.auge@lip6.fr', 'AuthString': 'demo'})
    result = router.forward(Query(*query), user=user)
    slice = result[0]
    del slice['resource']
    print slice
