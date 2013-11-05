#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class overrides Credential class provided by SFA
# in order to fix a bug in the current SFA implementation.
#
# Loic Baron        <loic.baron@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from sfa.trust.credential               import Credential as SFACredential
from manifold.util.log                  import Log

class Credential(SFACredential):
    def sfa_trust_credential_delegate(self, delegee_gidfile, caller_keyfile, caller_gidfile):
        """
        ???
        Args:
            delegee_gidfile: ??
            caller_keyfile:  ??
            caller_gidfile:  ??
        Returns:
            A delegated copy of this Credential, delegated to the specified gid's user.    
        """
        # get the gid of the object we are delegating
        object_gid = self.get_gid_object()
        object_hrn = object_gid.get_hrn()

        # the hrn of the user who will be delegated to
        # @loic corrected
        Log.debug("gid type = ",type(delegee_gidfile))
        Log.debug(delegee_gidfile.__class__)
        if not isinstance(delegee_gidfile, GID):
            delegee_gid = GID(filename=delegee_gidfile)
        else:
            delegee_gid = delegee_gidfile
        delegee_hrn = delegee_gid.get_hrn()

        #user_key = Keypair(filename=keyfile)
        #user_hrn = self.get_gid_caller().get_hrn()
        subject_string = "%s delegated to %s" % (object_hrn, delegee_hrn)
        dcred = Credential(subject = subject_string)
        dcred.set_gid_caller(delegee_gid)
        dcred.set_gid_object(object_gid)
        dcred.set_parent(self)
        dcred.set_expiration(self.get_expiration())
        dcred.set_privileges(self.get_privileges())
        dcred.get_privileges().delegate_all_privileges(True)
        #dcred.set_issuer_keys(keyfile, delegee_gidfile)
        dcred.set_issuer_keys(caller_keyfile, caller_gidfile)
        dcred.encode()
        dcred.sign()
        return dcred
