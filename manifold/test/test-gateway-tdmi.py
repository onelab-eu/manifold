#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Tests for TDMI Gateway
#
# Usage:
#  ./test-gateway-tdmi.py
#
# To turn on debug messages:
#  ./test-gateway-tdmi.py -d manifold -L DEBUG
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import sys

from manifold.bin.common    import MANIFOLD_STORAGE, run_commands, check_platform, MESSAGE_TO_ENABLE_PLATFORM
from manifold.bin.shell     import Shell
from manifold.util.log      import Log
from manifold.util.options  import Options
from manifold.util.type     import accepts, returns 

MESSAGE_TO_ADD_TDMI = """
tdmi has not been found in the Manifold Storage, please run:

    manifold-add-platform "tdmi" "Tophat Dedicated Measurement Infrastructure" "TDMI" "none" '{"db_host": "132.227.62.103", "db_port": 5432, "db_user": "postgres", "db_password": null, "db_name": "tophat", "name" : "TopHat team", "mail_support_address" : "xxx@xxx" }' 1

"""

# Prepare TDMI Queries
COMMANDS = [
"""
SELECT agent_id, ip
FROM agent
WHERE agent_id == 11824
""",
    'SELECT destination_id, ip FROM destination WHERE agent_id == 1417',
#    'SELECT src_ip, dst_ip, agent.ip, destination.ip, hops.ip, hops.ttl AT "2012-09-09 14:30:09" FROM traceroute WHERE agent_id == 11824 AND destination_id == 1417',
#    'SELECT agent.ip, src_ip, agent.hostname, destination.ip, dst_ip, destination.hostname AT "2012-09-09 14:30:09" FROM traceroute WHERE agent_id == 11824 AND destination_id INCLUDED [1416, 1417]'
]

@returns(int)
def main():
    argc = len(sys.argv)
    Shell.init_options()
    Log.init_options()
    Options().parse()

    shell = Shell(interactive = False)

    try:
        # Check whether tdmi is configured and enabled in Manifold
        check_platform(shell, "tdmi", MESSAGE_TO_ADD_TDMI, MESSAGE_TO_ENABLE_PLATFORM)
        ret = 0 if run_commands(shell, COMMANDS) else 2

    except Exception, e:
        # Handle exception which might have occurs and leave the shell gracefully
        Log.error(e)
        shell.terminate()
        ret = 1

    sys.exit(ret)

if __name__ == '__main__':
    main()
