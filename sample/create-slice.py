#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from config import auth

query = ('create', 'slice', [], {'slice_hrn': 'ple.upmc.myslicedemo2'}, [])

from tophat.core.router import THLocalRouter
from tophat.core.router import Query

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate(auth)
    result = router.forward(Query(*query), execute=True, user=user)
    print_result(result)
