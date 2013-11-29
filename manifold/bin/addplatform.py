#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.core.query    import Query
from manifold.core.router   import Router

def usage(gateways):
    print "Usage: %s NAME LONGNAME GATEWAY AUTH_TYPE CONFIG [DISABLED]" % sys.argv[0]
    print ""
    print "Add a platform to Manifold"
    print "    NAME: short name of the platform (lower case)."
    print "    LONGNAME: long name of the platform."
    print "    GATEWAY: type of gateway. Supported values: %s" % ', '.join(gateways) if gateways else '(unavailable)'
    print "    AUTH_TYPE: authentification type. Supported values: 'none', 'default', 'user'"
    print "    CONFIG: {'param':'value'}"
    print "    DISABLED: 0=enabled, 1=disabled"

def main():
    argc = len(sys.argv)
    program_name = sys.argv[0]

    with Router() as router:

        ret_gateways = router.receive(Query.get('local:gateway').select('name'))
        if ret_gateways['code'] != 0:
            print "W: Could not contact the Manifold server to list available gateways. Check disabled."
            supported_gateways = None
        else:
            gateways = ret_gateways['value']
            if not gateways:
                print "W: Could not contact the Manifold server to list available gateways. Check disabled."
                supported_gateways = None
            else:
                supported_gateways = [gw['name'] for gw in gateways]

        # Check number of arguments
        if argc not in [6, 7]:
            print >> sys.stderr, "%s: Invalid number of arguments (is equal to %d, should be equal to 6 or 7) " % (program_name, argc)
            usage(supported_gateways)
            sys.exit(1)

        name, longname, gateway, auth_type, config = sys.argv[1:6]
        if argc == 7:
            disabled = sys.argv[6]
        else:
            disabled = "1"


        # Check NAME
        if name != name.lower():
            print >> sys.stderr, "%s: Invalid NAME parameter (is '%s', should be '%s')" % (program_name, name, name.lower())
            usage()
            sys.exit(1)

        # Check GATEWAY
        if supported_gateways and gateway not in supported_gateways:
            print >> sys.stderr, "%s: Invalid GATEWAY parameter (is '%s', should be in '%s')" % (program_name, gateway, "', '".join(supported_gateways))
            usage()
            sys.exit(1)
            
        # Check AUTH_TYPE
        supported_auth_type = ["none", "default", "user"]
        if auth_type not in supported_auth_type:
            print >> sys.stderr, "%s: Invalid AUTH_TYPE parameter (is '%s', should be in '%s')" % (program_name, auth_type, "', '".join(supported_auth_type))
            usage()
            sys.exit(1)

        # Check DISABLED
        if argc == 7:
            supported_disabled = ["False", "FALSE", "NO", "0", "True", "TRUE", "YES", "1"]
            if str(sys.argv[6]) not in supported_disabled:
                print >> sys.stderr, "%s: Invalid DISABLED parameter (is '%s', should be in '%s')" % (program_name, disabled, "', '".join(supported_disabled))
                usage()
                sys.exit(1)

        disabled = str(sys.argv[6]) in ["False", "FALSE", "NO", "0"]

        platform_params = {
            'platform': name,
            'platform_longname': longname,
            'gateway_type': gateway,
            'auth_type':auth_type,
            'config': config,
            'disabled': disabled
        }

        router.receive(Query.create('local:platform').set(platform_params))

if __name__ == "__main__":
    main()
