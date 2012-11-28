#!/usr/bin/env python
#! -*- coding: utf-8 -*-

PLE_API = "https://www.planet-lab.eu/PLCAPI/"
REG_URL = 'http://www.planet-lab.eu:12345'

INTERFACE_HRN = 'ple'
MYSLICE_HRN='ple.upmc.slicebrowser'
#MYSLICE_API = "http://localhost:7080"
#MYSLICE_API = "http://demo.myslice.info:7080"

import sys
import os.path
import xmlrpclib
import getpass

# We should be able to package all these modules here
from sfa.trust.credential import Credential
from sfa.trust.gid import GID # this should be done into bootstrap
from sfa.client.sfaclientlib import SfaClientBootstrap
from sfa.planetlab.plxrn import hostname_to_hrn, slicename_to_hrn, email_to_hrn, hrn_to_pl_slicename
from sfa.util.xrn import get_authority

class SfaHelper:

    def __init__(self, user_hrn, private_key, sfi_dir):
        self.user_hrn = user_hrn
        self.private_key = private_key
        self.sfi_dir = sfi_dir

    # init self-signed cert, user credentials and gid
    def bootstrap (self):
        bootstrap = SfaClientBootstrap (self.user_hrn, REG_URL, self.sfi_dir)
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
        myslice_type='user'
        cred = self.bootstrap.delegate_credential_string (original_credential, MYSLICE_HRN, myslice_type)
        return cred

def get_credentials(pl_username, private_key, sfi_dir, password):
    # Getting user from PLE
    auth = {'AuthMethod': 'password', 'Username': pl_username, 'AuthString': password}
    ple = xmlrpclib.Server(PLE_API, allow_none = 1)
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
    site_hrn = ".".join([INTERFACE_HRN, sites[0]['login_base']])

    user_hrn = email_to_hrn(site_hrn, pl_username)

    # Getting slices from PLE
    slices = ple.GetSlices(auth, {}, ['name', 'person_ids'])
    slices = [s for s in slices if person_id in s['person_ids']]

    # Delegating user account and slices
    sfa = SfaHelper(user_hrn, private_key, sfi_dir)
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
        s_hrn = slicename_to_hrn(INTERFACE_HRN, s['name'])
        c = {
            'target': s_hrn,
            'type': 'slice',
            'cred': sfa.delegate('slice', s_hrn)
        }
        creds.append(c)
    return creds


def usage():
    print "Usage: %s PL_USERNAME PRIVATE_KEY SFI_DIR API_USERNAME API_URL" % sys.argv[0]
    print ""
    print "Delegates control to MySlice"
    print "    PL USERNAME"
    print "    PRIVATE KEY"
    print "    SFI_DIR       : default ~/.sfi"
    print "    API_USERNAME"
    print "    API_URL       : e.g: http://demo.myslice.info:7080"
    print ""
    print "Note: USER_PRIVATE_KEY is the name of the file containing your private key inside the .sfi directory as given by SFI_DIR."

def main():
    argc = len(sys.argv)
    if argc != 6:
        usage()
        sys.exit(1)

    pl_username, private_key = sys.argv[1:3]
    sfi_dir = sys.argv[3]
    if sfi_dir[-1] != '/':
        sfi_dir = sfi_dir + '/'
    sfi_dir = os.path.expanduser(sfi_dir)
    api_username = sys.argv[4]
    MYSLICE_API = sys.argv[5]
    password = getpass.getpass("Enter your PlanetLab password: ")
    api_password = getpass.getpass("Enter your API password: ")

    creds = get_credentials(pl_username, private_key, sfi_dir, password)

    # Uploading credentials to MySlice
    auth = {'AuthMethod': 'password', 'Username': api_username, 'AuthString': api_password}
    #print "W: delegation to demo user"
    #auth = {'AuthMethod': 'password', 'Username': 'demo', 'password': 'demo'}

    try:
        MySlice = xmlrpclib.Server(MYSLICE_API, allow_none = 1)
        for c in creds:
            MySlice.AddCredential(auth, c, 'ple')
    except Exception, e:
        print "E: Error uploading credential: %s" % e

if __name__ == '__main__':
    main()
