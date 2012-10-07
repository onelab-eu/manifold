#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'slice', [['authority_hrn', '=', 'ple.upmc']], {}, ['slice_hrn']) #, 'resource.hostname', 'resource.city'])


def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    #cpt = 0
    #for i in result['resource']:
    #    if cpt == 5:
    #        break
    #    print "  - %s - %s " % (i['hostname'], i['city'] if 'city' in i else 'N/A')
    #    cpt += 1
    #print "    (only 5 first displayed)"

def print_result(result):
    cpt = 0
    for i in result:
        #if cpt == 5:
        #    break
        print_slice(i)
        cpt += 1
    #print "(only 5 first displayed)"
    #print "============================="


from tophat.core.router import THLocalRouter
from tophat.core.router import Query
from delegation import *

pl_username = 'jordan.auge@lip6.fr'
private_key = '~/.ssh/id_rsa'
sfi_dir = '~/.sfi'

if sfi_dir[-1] != '/':
    sfi_dir = sfi_dir + '/'
sfi_dir = os.path.expanduser(sfi_dir)
password = getpass.getpass("Enter your password: ")

creds = get_credentials(pl_username, private_key, sfi_dir, password)

# Instantiate a TopHat router
with THLocalRouter() as router:
    for c in creds:
        router.add_credential(c)
    result = router.forward(Query(*query)) #, execute=False)
    if result:
        print ""
        print "=== RESULT ==="
        print_result(result)
        print "--------------"
        print ""
