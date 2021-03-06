#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys
import getpass
from hashlib import md5
import time
from random import Random
import crypt

from manifold.core.router import Router
from manifold.core.query import Query


def usage():
    print "Usage: %s EMAIL" % sys.argv[0]
    print ""
    print "Update the password of a user in Manifold local storage"
    print "    EMAIL: email address that identifies the user"
    print '    PASSWORD: "xxx"'

def main():
    argc = len(sys.argv)
    if argc != 2:
        usage()
        sys.exit(1)

    email = sys.argv[1]
    password = getpass.getpass("Password: ")

    magic = "$1$"

    password = password
    # Generate a somewhat unique 8 character salt string
    salt = str(time.time()) + str(Random().random())
    salt = md5(salt).hexdigest()[:8]

    if len(password) <= len(magic) or password[0:len(magic)] != magic:
        password = crypt.crypt(password.encode('latin1'), magic + salt + "$")

    user_params = {
        'password': password,
    }
    query = Query(action='update', object='local:user', params=user_params).filter_by('email', '==', email)

    # Instantiate a TopHat router
    with Router() as router:
        router.forward(query)

if __name__ == '__main__':
    main()
