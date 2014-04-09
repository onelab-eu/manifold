#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Add a platform in the Manifold Storage.
#
# Example:
#   cd /tmp
#   wget http://www.ece.gatech.edu/research/labs/MANIACS/as_taxonomy/data/as2attr.tgz
#   tar xzvf as2attr.txt.tgz
#   manifold-add-platform georgiatech "Georgia Tech Autonomous System Taxonomy Repository" csv none '{"asn":{"filename": "/tmp/as2attr.txt", "fields": [["asn", "int"], ["as_description", "string"], ["num_providers", "int"], ["num_peers", "int"], ["num_customers", "int"], ["num_prefixes_24", "int"], ["num_prefixes", "int"], ["asn_class", "string"]], "key": "asn"}}' 0
#   manifold-shell -z router
#
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys
from types                  import StringTypes

from manifold.bin.common    import check_num_arguments, check_option, check_option_bool, check_option_enum, run_command, string_to_bool
from manifold.core.local    import LOCAL_NAMESPACE
from manifold.util.type     import accepts, returns

DOC_ADD_PLATFORM = """
%(default_message)s

usage: %(program_name)s PLATFORM_NAME LONGNAME GATEWAY AUTH_TYPE CONFIG [DISABLED]

Add a platform to Manifold
    PLATFORM_NAME : A String containing the short name of the platform (lower case).
    LONGNAME      : A String containing the long name of the platform.
    GATEWAY_TYPE  : A String containing the type of Gateway.
    AUTH_TYPE     : Authentification type.
        Supported values are:
            "none"    : The platform support anonymous access.
            "default" :
            "user"    : The platform expect user's credentials.
    CONFIG        : A json encoded dict which may transports additional information
        related to this platform.
    DISABLED      : Disable this new platform in Manifold (default: 1)
        0: enabled
        1: disabled
"""

CMD_LIST_GATEWAYS = """
SELECT name
    FROM %(namespace)s:gateway
"""

CMD_ADD_PLATFORM = """
INSERT INTO %(namespace)s:platform
    SET
        platform          = '%(platform_name)s',
        platform_longname = '%(platform_longname)s',
        gateway_type      = '%(gateway_type)s',
        auth_type         = '%(auth_type)s',
        config            = '%(config)s',
        disabled          = %(disabled)s
"""

CMD_GET_GATEWAYS = """
SELECT type FROM %(namespace)s:gateway
"""

SUPPORTED_AUTH_TYPE = ["none", "default", "user"]

@returns(list)
def get_supported_gateway_types():
    """
    A list of String containing the name of each supported Gateway.
    """
    gateways = list()
    run_command(CMD_GET_GATEWAYS % {"namespace" : LOCAL_NAMESPACE}, gateways)
    return [gateway["type"] for gateway in gateways]

@returns(bool)
@accepts(StringTypes)
def is_lower_case(s):
    """
    Args:
        s: A String instance.
    Returns:
        True iif s is lower case.
    """
    return s == s.lower()

def main():
    check_num_arguments(DOC_ADD_PLATFORM, 6, 7)
    platform_name, platform_longname, gateway_type, auth_type, config = sys.argv[1:6]
    disabled = "1"
    argc = len(sys.argv)
    if argc == 7:
        disabled = sys.argv[6]

    # Check NAME
    check_option("PLATFORM_NAME", platform_name, is_lower_case)

    # Check GATEWAY
    supported_gateway_types = get_supported_gateway_types()
    if supported_gateway_types:
        check_option_enum("GATEWAY_TYPE", gateway_type, supported_gateway_types)

    # Check AUTH_TYPE
    check_option_enum("AUTH_TYPE", auth_type, SUPPORTED_AUTH_TYPE)

    # Check DISABLED
    if argc == 7:
        check_option_bool("DISABLED", disabled)
    disabled = string_to_bool(disabled)

    namespace = LOCAL_NAMESPACE
    return run_command(CMD_ADD_PLATFORM % locals())

if __name__ == "__main__":
    main()
