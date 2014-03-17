#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess

ROUTER = '/home/augej/repos/tophat/manifold/bin/router.py'

class Agent(object):
    def __init__(self):
        self._processes = dict()

    def run_router(self):
        p = subprocess.Popen(ROUTER, )
        self._processes['router'] = p
        return p

    def run_xmlrpc(self):
        pass

a = Agent()
p = a.run_router()
print p.pid
p.wait()
print "done"
