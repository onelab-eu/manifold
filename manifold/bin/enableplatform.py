#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.core.query import Query
from manifold.bin.shell  import Shell

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
    

    shell = Shell()

    # Using a query object...
    #platform_filters = [['platform', '=', name]]
    #platform_params = {'disabled': False}
    #query = Query(action='update', object='local:platform', filters=platform_filters, params=platform_params)
    #shell.execute(query)

    # ... or using SQL-like syntax.
    command = 'UPDATE local:platform SET disabled = False WHERE platform == "%(name)s"'
    shell.evaluate(command % locals())

    shell.terminate()


if __name__ == '__main__':
    main()
