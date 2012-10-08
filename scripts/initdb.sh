#!/bin/bash
# Delete the former DB
rm /var/myslice/db.sqlite
# This will initialize the DB (fails)
../sample/get-slice.py 1>/dev/null 2>/dev/null
# Insert dummy records
sqlite3 /var/myslice/db.sqlite  < ../sql/init.sql
# Make some delegations
./delegate.py jordan.auge@lip6.fr ~/.ssh/id_rsa ~/.sfi
