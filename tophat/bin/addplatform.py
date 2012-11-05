#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from tophat.core.router import THLocalRouter
from tophat.core.query import Query

def usage():
    print "Usage: %s NAME LONGNAME GATEWAY CONFIG [ENABLED]" % sys.argv[0]
    print ""
    print "Add a user to MySlice"
    print "    NAME: short name of the platform"
    print "    LONGNAME: long name of the platform"
    print "    ... TODO ..."

def main():
    argc = len(sys.argv)
    if argc not in [5,6]:
        usage()
        sys.exit(1)

    name, longname, gateway, config = sys.argv[1:5]
    disabled = sys.argv[5] in ['False', 'FALSE', 0, 'NO'] if argc == 6 else False
    
    platform_params = {
        'platform': name,
        'platform_longname': longname,
        'gateway_type': gateway,
        'config': config,
        'disabled': disabled
    }
    query = Query(action='create', fact_table='tophat:platform', params=platform_params)

    # Instantiate a TopHat router
    with THLocalRouter() as router:
        router.forward(query)

if __name__ == '__main__':
    main()
