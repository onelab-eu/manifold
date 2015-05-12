#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys
import getpass

from manifold.core.router import Router
from manifold.core.query import Query

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def create_ref_account(user, platform, ref_platform, router = None):
    account_params = {
        'user': user,
        'platform': platform,
        'auth_type': 'reference',
        'config': '{"reference_platform":"'+ref_platform+'"}'
    }

    account_query = Query(action='create', object='local:account', params=account_params)
    if router is None:
        with Router() as router:
            router.forward(account_query)
    else:
        router.forward(account_query)

def usage():
    print "Usage: %s PLATFORM REF_PLATFORM [USER]" % sys.argv[0]
    print ""
    print "PLEASE BACKUP THE DB BEFORE USING THIS COMMAND"
    print ""
    print "Add a reference account to ALL users for a recently added platform"
    print "    ... TODO ..."

def main():
    argc = len(sys.argv)
    if argc < 3:
        usage()
        sys.exit(1)

    print "PLEASE BACKUP THE DB BEFORE USING THIS COMMAND"
    answer = query_yes_no("Are you sure you want to continue?")
    if answer:
        platform = sys.argv[1]
        ref_platform = sys.argv[2]
        if argc == 4:
            user = sys.argv[3]
            create_ref_account(user, platform, ref_platform)
        else:
            # Get ALL active users
            query = Query().get('local:user').select('email').filter_by('status','==',2)
            with Router() as router:
                results = router.forward(query)
            for r in results['value']:
                create_ref_account(r['email'], platform, ref_platform, router)
    else:
        print "Exit"

if __name__ == '__main__':
    main()
