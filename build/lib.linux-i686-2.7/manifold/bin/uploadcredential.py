#!/usr/bin/env python
#! -*- coding: utf-8 -*-

DEFAULT_MYSLICE_API = "http://demo.myslice.info:7080"

from optparse import OptionParser

import sys
import xmlrpclib
import getpass
import traceback

def main():
    usage="""%prog [--url apiurl] platform api_username credential_1 .. credential_n
  Uploads a set of credentials to MySlice
Example:
  %prog ple my_login ~/.sfi/*_for*.cred"""
    parser=OptionParser (usage=usage)
    parser.add_option ("-u","--url",dest='url',default=DEFAULT_MYSLICE_API,
                       help="Specify API url (default is %s)"%DEFAULT_MYSLICE_API)
    (options,args)=parser.parse_args()

    if len(args)<=2: 
        parser.print_help()
        sys.exit(1)

    platform=args.pop(0)
    api_username=args.pop(0)
    delegated_credential_files=args

    #api_password = getpass.getpass("Enter your API password for %s: "%api_username)
    api_password = '69803641'
    auth = {'AuthMethod': 'password', 'Username': api_username, 'AuthString': api_password}

    for delegated_credential_file in delegated_credential_files:
        try:
            delegated_credential = open(delegated_credential_file).read()
            # Uploading credentials to MySlice
            MySlice = xmlrpclib.Server(options.url, allow_none = 1)

            query = {
                'object': 'local:platform',
                'filters'   : [['platform', '=', platform]],
                'fields'    : ['platform_id']
            }
            platforms = MySlice.Get(auth, query)
            if not platforms:
                raise Exception, "Not platform named '%s'" % platform
            platform_id = platforms[0]['platform_id']

            # Params depend on the credential type

            query = {
                'object': 'local:account',
                'filters'   : [['platform_id', '=', platform_id]],
                'params'    : {'credential': delegated_credential}
            }
            retcod = MySlice.Update(auth, query)
            #retcod=MySlice.AddCredential(auth, delegated_credential, platform)

            print delegated_credential_file,'upload retcod',retcod
        except Exception, e:
            print "E: Error uploading credential %s: %s" % (delegated_credential_file,e)
            traceback.print_exc()

if __name__ == '__main__':
    main()
