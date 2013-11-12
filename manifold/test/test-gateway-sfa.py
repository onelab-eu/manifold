#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Tests for SFA Gateways (RM and AM)
#

import cfgparse, pprint, sys, traceback
import random, string
from manifold.bin.shell  import Shell

from sfa.util.xrn                   import Xrn

from manifold.core.query            import Query
from manifold.core.receiver         import Receiver 
from manifold.core.router           import Router
from manifold.util.log              import Log
from manifold.util.options          import Options

def usage():
    print "Usage: %s" % sys.argv[0]

def random_char(y):
    return ''.join(random.choice(string.ascii_letters) for x in range(y))

def main():
    argc = len(sys.argv)
    if argc != 1:
        usage()
        sys.exit(1)

    # variables for UPDATE slice Query
    resource_urn = "urn:publicid:IDN+ple:upmc+node+ple4.ipv6.lip6.fr"

    # variables for INSERT INTO slice Query
    # this will create a slice with a Random name with 10 characters
    # Need to check if Metadata match with the Queries (ex: slice_hrn vs hrn / slice_urn vs urn)
    # DELETE Query will delete the newly created slice, Let's hope it will not match a previously existing slice...
    # Please delete it manually from the SFA Registry if DELETE Query fails !
    slice_hrn = "ple.upmc.%s" % (random_char(10))
    slice_urn = Xrn(slice_hrn, "slice").get_urn()

    queries = [
        # RM::get
#        'SELECT * FROM user      WHERE user_hrn      == "ple.upmc.loic_baron"',
#        'SELECT * FROM slice     WHERE slice_hrn     == "ple.upmc.myslicedemo"',
#        'SELECT * FROM authority WHERE authority_hrn == "ple.upmc"'#,
#        'UPDATE slice SET resource = ["%s"] WHERE slice_hrn == "ple.upmc.myslicedemo"' % (resource_urn)#, # sent to SFA AM
        'INSERT INTO slice SET slice_hrn = "%s", slice_urn = "%s", enabled = True' % (slice_hrn,slice_urn), # sent to SFA Registry
        'DELETE FROM slice WHERE slice_hrn = %s' % (slice_hrn)
    ]

    shell = Shell(interactive = True)

    for query in queries:
        shell.evaluate(query)
    shell.terminate()

if __name__ == '__main__':
    main()
