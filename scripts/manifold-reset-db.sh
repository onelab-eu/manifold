#!/bin/bash

# See ./manifold/bin/constants.py
STORAGE_FILENAME="/var/lib/manifold/storage.sqlite"

# Delete the former DB
rm -f $STORAGE_FILENAME

# Initialize the DB...
manifold-init-db

# ...and insert dummy records
sqlite3 $STORAGE_FILENAME < $1

# Delegation information
echo <<EOF

Before being able to use MySlice, you need to upload delegated credentials. We
provide a script for PlanetLab Europe users:"

$ myslice-sfa-delegate PL_USERNAME PRIVATE_KEY SFI_DIR

Example:
$ myslice-sfa-delegate john.doe@foo.fr ~/.ssh/id_rsa ~/.sfi

EOF
