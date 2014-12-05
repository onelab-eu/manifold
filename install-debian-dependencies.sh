#!/bin/bash

apt-get install `grep ^Depends stdeb.cfg | cut -d " " -f 2- | sed "s/,//g"`
pip install autobahn
