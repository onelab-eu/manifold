#!/usr/bin/env python
# -*- coding: utf-8 -*-

GIT_URL="git@systemx.enst.fr:ndn-simulations.git"

from manifold import initialize_meta

# Cannot use original manifold objects after this
initialize_meta()
from manifold.objects import Git

#MANIFOLD_ASSERT(Git, 'up_to_date', 'pull')

for g in Git.collection(GIT_URL):
    print g.log

#if not g.up_to_date():
#    g.pull()

#on(g.log(:msg, :committer), g.display("XX has committed YY"), POLL=S(1))
