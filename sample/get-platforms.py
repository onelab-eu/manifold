#!/usr/bin/env python
# -*- coding:utf-8 */

from tophat.core.router import THQuery, THLocalRouter as Router

query = THQuery('tophat:platforms', [], ['platform', 'platform_longname'])

# Instantiate a TopHat router
with Router() as router:
    result = router.forward(query)
    print result
