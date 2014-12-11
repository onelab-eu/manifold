#!/bin/bash
git pull; killall -9 manifold-agent || true && rm -f /var/run/manifold/manifold.sock && make install && manifold-agent -n
