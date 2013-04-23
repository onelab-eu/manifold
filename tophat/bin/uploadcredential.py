#!/usr/bin/env python
#! -*- coding: utf-8 -*-

# XXX
# Thierry April 2013
# This code is mostly useful only in conjunction with sfa
# For this reason I am moving it right into the sfi code,
# further changes here are probably not very useful
# see the ManifoldUploader class in sfa/client/manifolduploader.py
# XXX

DEFAULT_MYSLICE_API = "http://demo.myslice.info:7080"

# xxx
# this now has a user option (--old) so we can use either AddCredential (legacy)
# or Update() (new/current) to update credentials
# this option however should only be temporary as we could either
# (*) sense the flavour of the API we talk to (is there a GetVersion ?)
# (*) or drop support for old APIs altogether on the longer run

from optparse import OptionParser

import sys
import xmlrpclib
import getpass

def main():
    usage="""%prog [options] platform api_username credential_1 .. credential_n
  Uploads a set of credentials to MySlice
Example:
  %prog ple my_login ~/.sfi/*_for*.cred"""
    parser=OptionParser (usage=usage)
    parser.add_option ("-u","--url",dest='url',action='store',default=DEFAULT_MYSLICE_API,
                       help="Specify API url (default is %s)"%DEFAULT_MYSLICE_API)
    parser.add_option ("-o","--old",action='store_true',dest='old_api',default=False,
                       help="if your backend runs old API (using AddCredential)")
    parser.add_option ("-n","--new",action='store_false',dest='old_api',
                       help="default -- if your backend runs new API (use Update)")
    (options,args)=parser.parse_args()

    if len(args)<=2: 
        parser.print_help()
        sys.exit(1)

    platform=args.pop(0)
    api_username=args.pop(0)
    delegated_credential_files=args

    api_password = getpass.getpass("Enter your API password for %s: "%api_username)
    auth = {'AuthMethod': 'password', 'Username': api_username, 'AuthString': api_password}

    for delegated_credential_file in delegated_credential_files:
        try:
            delegated_credential = open(delegated_credential_file).read()
            manifold = xmlrpclib.Server(options.url, allow_none = 1)
            if options.old_api:
                # Uploading credentials to MySlice
                retcod=manifold.AddCredential(auth, delegated_credential, platform)
                if retcod==1:
                    print delegated_credential_file,'upload OK'
                else:
                    print delegated_credential_file,'upload retcod',retcod
            else:
                query= { 'action':       'update',
                         'fact_table':   'local:account',
                         'filters':      [ ['platform', '=', platform] ] ,
                         'params':       {'credential': delegated_credential, },
                         }
                retcod=manifold.Update (auth, query)
                if retcod['code']==0:
                    print delegated_credential_file,'upload OK'
                else:
                    print delegated_credential_file, "upload failed,",retcod['output'], \
                        "with code",retcod['code']
        except Exception, e:
            print "E: Error uploading credential %s: %s" % (delegated_credential_file,e)
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    main()
