#!/bin/bash
# Delete the former DB
rm /var/myslice/db.sqlite
# Initialize the DB
./init-db.py
# Insert dummy records
sqlite3 /var/myslice/db.sqlite  < ../sql/init.sql
# Make some delegations
print "./delegate.py jordan.auge@lip6.fr ~/.ssh/id_rsa ~/.sfi"
./delegate.py jordan.auge@lip6.fr ~/.ssh/id_rsa ~/.sfi
