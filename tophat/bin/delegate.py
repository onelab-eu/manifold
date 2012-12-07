#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys
import os.path
import xmlrpclib
import getpass

from optparse import OptionParser

# We should be able to package all these modules here
from sfa.trust.credential import Credential
from sfa.trust.gid import GID # this should be done into bootstrap
from sfa.client.sfaclientlib import SfaClientBootstrap
from sfa.planetlab.plxrn import hostname_to_hrn, slicename_to_hrn, email_to_hrn, hrn_to_pl_slicename
from sfa.util.xrn import get_authority

class SfaHelper:

    def __init__(self, user_hrn, private_key, sfi_dir, reg_url, myslice_hrn, myslice_type):
        self.user_hrn = user_hrn
        self.private_key = private_key
        self.sfi_dir = sfi_dir
        self.reg_url = reg_url
        self.myslice_hrn = myslice_hrn
        self.myslice_type = myslice_type

    # init self-signed cert, user credentials and gid
    def bootstrap (self):
        bootstrap = SfaClientBootstrap (self.user_hrn, self.reg_url, self.sfi_dir)
        # if -k is provided, use this to initialize private key
        if self.private_key:
            bootstrap.init_private_key_if_missing (self.private_key)
        else:
            # trigger legacy compat code if needed 
            # the name has changed from just <leaf>.pkey to <hrn>.pkey
            if not os.path.isfile(bootstrap.private_key_filename()):
                print "I: private key not found, trying legacy name"
                try:
                    legacy_private_key = os.path.join (self.sfi_dir, "%s.pkey"%get_leaf(self.user_hrn))
                    self.logger.debug("legacy_private_key=%s"%legacy_private_key)
                    bootstrap.init_private_key_if_missing (legacy_private_key)
                    print "I: Copied private key from legacy location %s"%legacy_private_key
                except:
                    print "E: Can't find private key"
                    sys.exit(1)
            
        # make it bootstrap
        bootstrap.bootstrap_my_gid()
        # extract what's needed
        self.private_key = bootstrap.private_key()
        self.my_credential_string = bootstrap.my_credential_string () # do once !
        self.my_gid = bootstrap.my_gid ()
        self.bootstrap = bootstrap

    def delegate(self, delegate_type, delegate_name):
        """
        (locally) create delegate credential for use by given hrn
        """

        if delegate_type == 'user':
            # done in bootstrap
            print "I: delegate user", delegate_name
            original_credential = self.my_credential_string

        elif delegate_type == 'slice':
            print "I: delegate slice", delegate_name
            original_credential = self.bootstrap.slice_credential_string(delegate_name)

        # this is for when you need to act as a PI
        elif delegate_type == 'authority':
            print "I: delegate authority", delegate_name
            original_credential = self.bootstrap.authority_credential_string(delegate_name)

        else:
            print "E: Must specify either --user or --slice <hrn>"
            return

        # we implicitly assume that say, ple.upmc.slicebrowser always is a user
        # could as well have been an authority but for now.. 
        cred = self.bootstrap.delegate_credential_string (original_credential, self.myslice_hrn, self.myslice_type)
        return cred

def get_credentials(pl_username, private_key, sfi_dir, pl_password, plcapi_url, reg_url, interface_hrn, myslice_hrn, myslice_type):
    # Getting user from PLE
    auth = {'AuthMethod': 'password', 'Username': pl_username, 'AuthString': pl_password}
    ple = xmlrpclib.Server(plcapi_url, allow_none = 1)
    persons =  ple.GetPersons(auth, {'email': pl_username}, ['person_id', 'site_ids'])
    if not persons:
        raise Exception, "User not found."
        sys.exit(1)
    person_id = persons[0]['person_id']
    site_id = persons[0]['site_ids'][0]

    # Getting site from PLE
    sites = ple.GetSites(auth, {'site_id': site_id}, ['login_base'])
    if not sites:
        raise Exception, "Site not found"
        sys.exit(1)
    site_hrn = ".".join([interface_hrn, sites[0]['login_base']])

    user_hrn = email_to_hrn(site_hrn, pl_username)

    # Getting slices from PLE
    print "I: retrieving slices at %s"%plcapi_url
    slices = ple.GetSlices(auth, {}, ['name', 'person_ids'])
    slices = [s for s in slices if person_id in s['person_ids']]

    # Delegating user account and slices
    sfa = SfaHelper(user_hrn, private_key, sfi_dir, reg_url, myslice_hrn, myslice_type)
    sfa.bootstrap()

    creds = []

    c = {
        'target': user_hrn,
        'type': 'user',
        'cred': sfa.delegate('user', user_hrn)
    }
    creds.append(c)

    try:
        user_auth = get_authority(user_hrn)
        c = {
            'target': user_auth,
            'type': 'authority',
            'cred': sfa.delegate('authority', user_auth)
        }
        creds.append(c)
    except Exception:
        print "I: No authority credential."

    for s in slices:
        s_hrn = slicename_to_hrn(interface_hrn, s['name'])
        c = {
            'target': s_hrn,
            'type': 'slice',
            'cred': sfa.delegate('slice', s_hrn)
        }
        creds.append(c)
    return creds


####################
DEFAULT_MYSLICE_API = "http://demo.myslice.info:7080"
DEFAULT_PRIVATE_KEY=os.path.expanduser("~/.ssh/id_rsa")
DEFAULT_SFI_DIR=os.path.expanduser("~/.sfi/")
DEFAULT_PLC_API = "https://www.planet-lab.eu/PLCAPI/"
DEFAULT_REG_URL = 'http://www.planet-lab.eu:12345'
DEFAULT_MYSLICE_HRN='ple.upmc.slicebrowser'
DEFAULT_MYSLICE_TYPE='user'
DEFAULT_INTERFACE_HRN = 'ple'

def main():

    usage="""%prog [options] platform api_username pl_username
  Computes delegated credentials for you as a user and for your slices
  and uploads them on a MySlice backend

Example:
  %prog ple thierry thierry.parmentelat@inria.fr"""

    parser=OptionParser (usage=usage)
    parser.add_option ("-u","--url",dest='myslice_api',default=DEFAULT_MYSLICE_API,
                       help="Specify API url default is %s"%DEFAULT_MYSLICE_API)
    parser.add_option ("-m","--myslice-hrn",dest='myslice_hrn',default=DEFAULT_MYSLICE_HRN,
                       help="hrn that your myslice runs as, default is %s"%DEFAULT_MYSLICE_HRN)
    parser.add_option ("-t","--myslice-type",dest='myslice_type',default=DEFAULT_MYSLICE_TYPE,
                       help="the SFA type for MYSLICE_HRN, default is %s"%DEFAULT_MYSLICE_TYPE)
    parser.add_option ("-k","--key",dest='private_key',default=DEFAULT_PRIVATE_KEY,
                       help="Private key to use, default is %s"%DEFAULT_PRIVATE_KEY),
    parser.add_option ("-s","--sfi-dir",dest='sfi_dir',default=DEFAULT_SFI_DIR,
                       help="sfi directory default is %s"%DEFAULT_SFI_DIR)
    parser.add_option ("-r","--reg-url",dest='reg_url',default=DEFAULT_REG_URL,
                       help="URL for the registry interface, default is %s"%DEFAULT_REG_URL)
    parser.add_option ("-p","--plcapi-url",dest='plc_api',default=DEFAULT_PLC_API,
                       help="URL for the PLCAPI where Slices get fetched, default is %s"%DEFAULT_PLC_API)
    parser.add_option ("-i","--interface-hrn",dest='interface_hrn',default=DEFAULT_INTERFACE_HRN,
                       help="the toplevel HRN for your planetlab instance, default is %s"%DEFAULT_INTERFACE_HRN)
    
                       
    (options,args)=parser.parse_args()
    if len(args) != 3:
        parser.print_help()
        sys.exit(1)

    (platform, api_username, pl_username) = args
    sfi_dir = options.sfi_dir
    if sfi_dir[-1] != '/':
        sfi_dir = sfi_dir + '/'
    myslice_api = options.myslice_api
    pl_password = getpass.getpass("Enter the PlanetLab password for %s: "%pl_username)
    api_password = getpass.getpass("Enter your API password for %s: "%api_username)

    creds = get_credentials(pl_username, options.private_key, sfi_dir, pl_password, 
                            options.plc_api, options.reg_url, options.interface_hrn, 
                            options.myslice_hrn, options.myslice_type)

    # Uploading credentials to MySlice
    auth = {'AuthMethod': 'password', 'Username': api_username, 'AuthString': api_password}

    try:
        MySlice = xmlrpclib.Server(options.myslice_api, allow_none = 1)
        for c in creds:
            MySlice.AddCredential(auth, c, platform)
            print "I: uploading credential"
    except Exception, e:
        print "E: Error uploading credential: %s" % e

if __name__ == '__main__':
    main()
