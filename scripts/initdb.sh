#!/bin/bash
# Delete the former DB
rm /var/myslice/db.sqlite
# This will initialize the DB
../sample/get-slices.py
# Insert dummy records
sqlite3 /var/myslice/db.sqlite  < ../sql/init.sql
# Make some delegations
../sample/get-slice-do-delegation.py
