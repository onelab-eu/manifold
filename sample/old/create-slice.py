#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from config import auth

query = ('create', 'slice', [], {'slice_hrn': 'ple.upmc.myslicedemo3'}, [])

from manifold.core.router import THRouter
from manifold.core.router import Query

# Instantiate a TopHat router
with THRouter() as router:
    user = router.authenticate(auth)
    result = router.forward(Query(*query), execute=True, user=user)
    print result
