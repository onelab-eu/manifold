#!/usr/bin/env python
# -*- coding: utf-8 -*-

GIT_URL="git@systemx.enst.fr:ndn-simulations.git"

# XXX what if "import manifold" (put in cache, not loaded again ?)
from manifold import initialize_meta

# Setup handling of manifold imports
# Cannot use original manifold objects after this
initialize_meta()

from manifold.objects import Git

# Check whether an object has the necessary attributes for the script
#MANIFOLD_ASSERT_ATTRIBUTES(Git, 'up_to_date', 'pull')

for g in Git.collection(url=GIT_URL).prefetch('url', 'log'):
    print g.log

#if not g.up_to_date():
#    g.pull()

# In a daemon...
#on(g.log(:msg, :committer), g.display("XX has committed YY"), POLL=S(1))
