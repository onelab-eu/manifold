#!/usr/bin/env python

import time
from manifold.bin.shell import Shell

NAMESPACE = 'iotlab'
SLICE_HRN = 'ple.upmc.myslicedemo'

GRAIN = 1800
IOTLAB_NODE_1 = 'urn:publicid:IDN+iotlab+node+a8-6.rocquencourt.iot-lab.info'

t = time.time()
START = int(t - (t % GRAIN) + GRAIN)

LIST_RESOURCES        = 'SELECT hrn FROM resource'
LIST_LEASES           = 'SELECT resource, start_time, end_time, duration FROM lease'
LIST_SLICE_RESOURCES  = 'SELECT hrn FROM resource WHERE slice == "%s"' % (SLICE_HRN, )
LIST_SLICE_LEASES     = 'SELECT resource, start_time, end_time, duration FROM lease WHERE slice == "%s"' % (SLICE_HRN, )

# sfa/iotlab/iotlabshell.py:    _MINIMUM_DURATION = 10  # 10 units of granularity 60 s, 10 mins
# 1800 too much !!!!
ADD_LEASE             = 'UPDATE slice SET resource = ["%s"], lease = [{resource: "%s", start_time: %d, duration: 10}] where slice_hrn == "%s"' % (IOTLAB_NODE_1, IOTLAB_NODE_1, START, SLICE_HRN, )


################################################################################

def run_command(command):
   s.display(s.evaluate(command))

################################################################################

s = Shell(interactive=False)
s.select_auth_method('local')

#run_command(LIST_RESOURCES)
run_command(LIST_LEASES) # Possibility to have a hardcoded value
#run_command(LIST_SLICE_RESOURCES)
#run_command(LIST_SLICE_LEASES)

# Lease is not display at the end when the command succeeds
#run_command(ADD_LEASE)

s.terminate()
