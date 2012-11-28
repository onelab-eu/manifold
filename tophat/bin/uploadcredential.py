#!/usr/bin/env python
#! -*- coding: utf-8 -*-

MYSLICE_API = "http://demo.myslice.info:7080"

import sys
import xmlrpclib
import getpass

def usage():
    print "Usage: %s CREDENTIAL PLATFORM API_USERNAME" % sys.argv[0]
    print ""
    print "Uploads a credential to MySlice"
    print "    CREDENTIAL"
    print "    PLATFORM"
    print "    API_USERNAME"

def main():
    argc = len(sys.argv)
    if argc != 4:
        usage()
        sys.exit(1)

    credential = open(sys.argv[1]).read()
    platform = sys.argv[2]
    api_username = sys.argv[3]
    api_password = getpass.getpass("Enter your API password: ")

    # Uploading credentials to MySlice
    auth = {'AuthMethod': 'password', 'Username': api_username, 'AuthString': api_password}

    try:
        MySlice = xmlrpclib.Server(MYSLICE_API, allow_none = 1)
        MySlice.AddCredential(auth, credential, platform)
    except Exception, e:
        print "E: Error uploading credential: %s" % e

if __name__ == '__main__':
    main()
