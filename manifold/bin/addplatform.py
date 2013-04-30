#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.core.router import Router
from manifold.core.query import Query

def usage():
    print "Usage: %s NAME LONGNAME GATEWAY AUTH_TYPE CONFIG [ENABLED]" % sys.argv[0]
    print ""
    print "Add a user to MySlice"
    print "    NAME: short name of the platform"
    print "    LONGNAME: long name of the platform"
    print "    GATEWAY: SFA, Manifold, OML,..."
    print "    AUTH_TYPE: user, none, default"
    print "    CONFIG: {'param':'value'}"
    print "    DISABLED: 0=enabled, 1=disabled"

def main():
    argc = len(sys.argv)
    print "arguments count = ",argc
    if argc not in [5,6]:
        usage()
        sys.exit(1)

    name, longname, gateway, auth_type, config = sys.argv[1:6]
    disabled = sys.argv[6] in ['False', 'FALSE', 0, 'NO'] if argc == 7 else '1'
    
    # @loic added gateway_conf
    # why 2 different fields for the same config ?
    platform_params = {
        'platform': name,
        'platform_longname': longname,
        'gateway_type': gateway,
        'gateway_conf':config,
        'auth_type':auth_type,
        'config': config,
        'disabled': disabled
    }
    query = Query(action='create', fact_table='local:platform', params=platform_params)

    # Instantiate a TopHat router
    with Router() as router:
        router.forward(query)

if __name__ == '__main__':
    main()
