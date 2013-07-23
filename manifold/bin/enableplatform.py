#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.bin.shell  import Shell

def usage():
    print "Usage: %s NAME" % sys.argv[0]
    print ""
    print "Enable a platform"

def main():
    argc = len(sys.argv)
    if argc != 2:
        usage()
        sys.exit(1)
    name = sys.argv[1]
    
    shell = Shell()

    command = 'UPDATE local:platform SET disabled = False WHERE platform == "%(name)s"'
    shell.evaluate(command % locals())

    # Equivalent using a query object...
    # 
    # from manifold.core.query import Query
    # platform_filters = [['platform', '=', name]]
    # platform_params = {'disabled': False}
    # query = Query(action='update', object='local:platform', filters=platform_filters, params=platform_params)
    # shell.execute(query)


    shell.terminate()


if __name__ == '__main__':
    main()
