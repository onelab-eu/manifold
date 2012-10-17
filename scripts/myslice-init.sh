#!/bin/bash

# Delete the former DB
rm -f /var/myslice/db.sqlite

# Initialize the DB...
myslice-init-db.py

# ...and insert dummy records
sqlite3 /var/myslice/db.sqlite < $1

# Delegation information
echo <<EOF

Before being able to use MySlice, you need to upload delegated credentials. We
provide a script for PlanetLab Europe users:"

$ myslice-sfa-delegate PL_USERNAME PRIVATE_KEY SFI_DIR

Example:
$ myslice-sfa-delegate jordan.auge@lip6.fr ~/.ssh/id_rsa ~/.sfi

EOF
