#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.core.router import THRouter
from manifold.core.query import Query

def usage():
    print "Usage: %s NAME" % sys.argv[0]
    print ""
    print "Disable a platform"

def main():
    argc = len(sys.argv)
    if argc != 2:
        usage()
        sys.exit(1)

    name = sys.argv[1]
    
    platform_filters = [['platform', '=', name]]
    platform_params = {'disabled': False}
    query = Query(action='update', fact_table='tophat:platform', filters=platform_filters, params=platform_params)

    # Instantiate a TopHat router
    with THRouter() as router:
        router.forward(query)

if __name__ == '__main__':
    main()
