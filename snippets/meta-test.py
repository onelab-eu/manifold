#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from meta import manifold_setup
manifold_setup('manifoldhttp://dev.myslice.info:7080')

from manifold import Resource, Slice

print("Resource = %r" % (Resource,))
r = Resource()
print("Resource() = %r" % (r,))

