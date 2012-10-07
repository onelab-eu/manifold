#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from tophat.core.router import THLocalRouter
from tophat.core.router import Query

query = ('get', 'slice', [], {}, ['slice_hrn', 'resource.hostname'])

# Instantiate a TopHat router
with THLocalRouter() as router:
    result = router.forward(Query(*query))
    slice = result[0]
    del slice['resource']
    print slice
