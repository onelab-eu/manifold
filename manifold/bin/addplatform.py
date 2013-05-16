#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys

from manifold.core.router import Router
from manifold.core.query import Query

def usage():
    print "Usage: %s NAME LONGNAME GATEWAY AUTH_TYPE CONFIG [DISABLED]" % sys.argv[0]
    print ""
    print "Add a platform to Manifold"
    print "    NAME: short name of the platform (lower case)."
    print "    LONGNAME: long name of the platform."
    print "    GATEWAY: type of gateway. Supported values: 'SFA', 'Manifold', 'OML', 'TDMI'..."
    print "    AUTH_TYPE: authentification type. Supported values: 'none', 'default', 'user'"
    print "    CONFIG: {'param':'value'}"
    print "    DISABLED: 0=enabled, 1=disabled"

def main():
    argc = len(sys.argv)
    program_name = sys.argv[0]

    # Check number of arguments
    if argc not in [6, 7]:
        print >> sys.stderr, "%s: Invalid number of arguments (is equal to %d, should be equal to 6 or 7) " % (program_name, argc)
        usage()
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
    supported_gateways = ["SFA", "Manifold", "OML", "TDMI"]
    if gateway not in supported_gateways:
        print >> sys.stderr, "%s: Invalid NAME parameter (is '%s', should be in '%s')" % (program_name, gateway, "', '".join(supported_gateways))
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

    # @loic added gateway_conf
    # why 2 different fields for the same config ?
    platform_params = {
        "platform"          : name,
        "platform_longname" : longname,
        "gateway_type"      : gateway,
        "gateway_conf"      : config,
        "auth_type"         : auth_type,
        "config"            : config,
        "disabled"          : disabled
    }

    # Add in the Manifold's storage (local) in the 'platform' table the
    # newly declared platform.
    query = Query(action="create", object="local:platform", params=platform_params)

    # Instantiate a TopHat router
    with Router() as router:
        router.forward(query)

if __name__ == "__main__":
    main()
