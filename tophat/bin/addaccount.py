#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys
import getpass

from tophat.core.router import THLocalRouter
from manifold.core.query import Query

def usage():
    print "Usage: %s USER PLATFORM TYPE CONFIG" % sys.argv[0]
    print ""
    print "Add an account to MySlice"
    print "    ... TODO ..."

def main():
    argc = len(sys.argv)
    if argc != 5:
        usage()
        sys.exit(1)

    user, platform, type, config = sys.argv[1:5]
    
    account_params = {
        'user': user,
        'platform': platform,
        'auth_type': type,
        'config': config
    }
    query = Query(action='create', fact_table='tophat:account', params=account_params)

    # Instantiate a TopHat router
    with THLocalRouter() as router:
        router.forward(query)

if __name__ == '__main__':
    main()
