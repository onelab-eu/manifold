#
# PLCAPI authentication parameters
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# $Id: Auth.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/PLC/Auth.py $
#

import crypt
from hashlib import sha1 as sha
import hmac
import time

from MySlice.Faults import *
from MySlice.Parameter import Parameter, Mixed
from MySlice.Nodes import Node, Nodes
from MySlice.Interfaces import Interface, Interfaces
from MySlice.Sessions import Session, Sessions
#from MySlice.Peers import Peer, Peers
from MySlice.Roles import Role, Roles
from MySlice.Boot import notify_owners
from MySlice.Person import Person, Persons

def map_auth(auth):
    if auth['AuthMethod'] == "session":
        expected = SessionAuth()
    elif auth['AuthMethod'] == "password" or \
         auth['AuthMethod'] == "capability":
        expected = PasswordAuth()
#    elif auth['AuthMethod'] == "gpg":
#        expected = GPGAuth()
    elif auth['AuthMethod'] == "hmac" or \
         auth['AuthMethod'] == "hmac_dummybox":
        expected = BootAuth()
    elif auth['AuthMethod'] == "anonymous":
        expected = AnonymousAuth()
    elif auth['AuthMethod'] == "ple":
        expected = PLEAuth()
    elif auth['AuthMethod'] == "plc":
        expected = PLCAuth()
    elif auth['AuthMethod'] == "guest":
        expected = GuestAuth()
    elif auth['AuthMethod'] == "managed":
        expected = ManagedAuth()
    else:
        raise PLCInvalidArgument("must be 'session', 'password', 'gpg', 'hmac', 'hmac_dummybox', 'ple', 'plc' or 'anonymous'", "AuthMethod")
    return expected

class Auth(Parameter):
    """
    Base class for all API authentication methods, as well as a class
    that can be used to represent all supported API authentication
    methods.
    """

    def __init__(self, auth = None):
        if auth is None:
            auth = {'AuthMethod': Parameter(str, "Authentication method to use", optional = False)}
        Parameter.__init__(self, auth, "API authentication structure")

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert 'AuthMethod' in auth

        expected = map_auth(auth)

        # Re-check using the specified authentication method
        method.type_check("auth", auth, expected, (auth,) + args)

#class GPGAuth(Auth):
#    """
#    Proposed PlanetLab federation authentication structure.
#    """
#
#    def __init__(self):
#        Auth.__init__(self, {
#            'AuthMethod': Parameter(str, "Authentication method to use, always 'gpg'", optional = False),
#            'name': Parameter(str, "Peer or user name", optional = False),
#            'signature': Parameter(str, "Message signature", optional = False)
#            })
#
#    def check(self, method, auth, *args):
#        try:
#            peers = Peers(method.api, [auth['name']])
#            if peers:
#                if 'peer' not in method.roles:
#                    raise PLCAuthenticationFailure, "Not allowed to call method"
#
#                method.caller = peer = peers[0]
#                keys = [peer['key']]
#            else:
#                persons = Persons(method.api, {'email': auth['name'], 'enabled': True, 'peer_id': None})
#                if not persons:
#                    raise PLCAuthenticationFailure, "No such user '%s'" % auth['name']
#
#                if not set(person['roles']).intersection(method.roles):
#                    raise PLCAuthenticationFailure, "Not allowed to call method"
#
#                method.caller = person = persons[0]
#                keys = Keys(method.api, {'key_id': person['key_ids'], 'key_type': "gpg", 'peer_id': None})
#
#            if not keys:
#                raise PLCAuthenticationFailure, "No GPG key on record for peer or user '%s'"
#
#            for key in keys:
#                try:
#                    from MySlice.GPG import gpg_verify
#                    gpg_verify(args, key, auth['signature'], method.name)
#                    return
#                except PLCAuthenticationFailure, fault:
#                    pass
#
#            raise fault
#
#        except PLCAuthenticationFailure, fault:
#            # XXX Send e-mail
#            raise fault

class SessionAuth(Auth):
    """
    Secondary authentication method. After authenticating with a
    primary authentication method, call GetSession() to generate a
    session key that may be used for subsequent calls.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'session'", optional = False),
            'session': Parameter(str, "Session key", optional = False)
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert auth.has_key('session')

        # Get session record
        sessions = Sessions(method.api, [auth['session']], expires = None)
        if not sessions:
            raise PLCAuthenticationFailure, "No such session"
        session = sessions[0]

        try:
            if session['node_id'] is not None:
                nodes = Nodes(method.api, {'node_id': session['node_id'], 'peer_id': None})
                if not nodes:
                    raise PLCAuthenticationFailure, "No such node"
                node = nodes[0]

                if 'node' not in method.roles:
                    raise PLCAuthenticationFailure, "Not allowed to call method"

                method.caller = node

            elif session['person_id'] is not None and session['expires'] > time.time():
                persons = Persons(method.api, {'person_id': session['person_id'], 'enabled': True, 'peer_id': None})
                if not persons:
                    raise PLCAuthenticationFailure, "No such account (SESS)"
                person = persons[0]

                if not set(person['roles']).intersection(method.roles):
                    raise PLCPermissionDenied, "Not allowed to call method"

                method.caller = persons[0]

            else:
                raise PLCAuthenticationFailure, "Invalid session"

        except PLCAuthenticationFailure, fault:
            session.delete()
            raise fault

class BootAuth(Auth):
    """
    PlanetLab version 3.x node authentication structure. Used by the
    Boot Manager to make authenticated calls to the API based on a
    unique node key or boot nonce value.

    The original parameter serialization code did not define the byte
    encoding of strings, or the string encoding of all other types. We
    define the byte encoding to be UTF-8, and the string encoding of
    all other types to be however Python version 2.3 unicode() encodes
    them.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'hmac'", optional = False),
            'node_id': Parameter(int, "Node identifier", optional = False),
            'value': Parameter(str, "HMAC of node key and method call", optional = False)
            })

    def canonicalize(self, args):
        values = []

        for arg in args:
            if isinstance(arg, list) or isinstance(arg, tuple):
                # The old implementation did not recursively handle
                # lists of lists. But neither did the old API itself.
                values += self.canonicalize(arg)
            elif isinstance(arg, dict):
                # Yes, the comments in the old implementation are
                # misleading. Keys of dicts are not included in the
                # hash.
                values += self.canonicalize(arg.values())
            else:
                # We use unicode() instead of str().
                values.append(unicode(arg))

        return values

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert auth.has_key('node_id')

        if 'node' not in method.roles:
            raise PLCAuthenticationFailure, "Not allowed to call method"

        try:
            nodes = Nodes(method.api, {'node_id': auth['node_id'], 'peer_id': None})
            if not nodes:
                raise PLCAuthenticationFailure, "No such node"
            node = nodes[0]

            if node['key']:
                key = node['key']
            elif node['boot_nonce']:
                # Allow very old nodes that do not have a node key in
                # their configuration files to use their "boot nonce"
                # instead. The boot nonce is a random value generated
                # by the node itself and POSTed by the Boot CD when it
                # requests the Boot Manager. This is obviously not
                # very secure, so we only allow it to be used if the
                # requestor IP is the same as the IP address we have
                # on record for the node.
                key = node['boot_nonce']

                interface = None
                if node['interface_ids']:
                    interfaces = Interfaces(method.api, node['interface_ids'])
                    for interface in interfaces:
                        if interface['is_primary']:
                            break
            
                if not interface or not interface['is_primary']:
                    raise PLCAuthenticationFailure, "No primary network interface on record"
            
                if method.source is None:
                    raise PLCAuthenticationFailure, "Cannot determine IP address of requestor"

                if interface['ip'] != method.source[0]:
                    raise PLCAuthenticationFailure, "Requestor IP %s does not match node IP %s" % \
                          (method.source[0], interface['ip'])
            else:
                raise PLCAuthenticationFailure, "No node key or boot nonce"

            # Yes, this is the "canonicalization" method used.
            args = self.canonicalize(args)
            args.sort()
            msg = "[" + "".join(args) + "]"

            # We encode in UTF-8 before calculating the HMAC, which is
            # an 8-bit algorithm.
            digest = hmac.new(key, msg.encode('utf-8'), sha).hexdigest()

            if digest != auth['value']:
                raise PLCAuthenticationFailure, "Call could not be authenticated"

            method.caller = node

        except PLCAuthenticationFailure, fault:
            if nodes:
                notify_owners(method, node, 'authfail', include_pis = True, include_techs = True, fault = fault)
            raise fault

class AnonymousAuth(Auth):
    """
    PlanetLab version 3.x anonymous authentication structure.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'anonymous'", False),
            })

    def check(self, method, auth, *args):
        if 'anonymous' not in method.roles:
            raise PLCAuthenticationFailure, "Not allowed to call method anonymously"

        method.caller = None

class PasswordAuth(Auth):
    """
    PlanetLab version 3.x password authentication structure.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'password' or 'capability'", optional = False),
            'Username': Parameter(str, "PlanetLab username, typically an e-mail address", optional = False),
            'AuthString': Parameter(str, "Authentication string, typically a password", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert auth.has_key('Username')

        # Get record (must be enabled)
        persons = Persons(method.api, {'email': auth['Username'].lower(), 'enabled': True, 'peer_id': None})
        if len(persons) != 1:
            raise PLCAuthenticationFailure, "No such account (PW)"

        person = persons[0]

        if auth['Username'] == method.api.config.PLC_API_MAINTENANCE_USER:
            # "Capability" authentication, whatever the hell that was
            # supposed to mean. It really means, login as the special
            # "maintenance user" using password authentication. Can
            # only be used on particular machines (those in a list).
            sources = method.api.config.PLC_API_MAINTENANCE_SOURCES.split()
            if method.source is not None and method.source[0] not in sources:
                raise PLCAuthenticationFailure, "Not allowed to login to maintenance account"

            # Not sure why this is not stored in the DB
            password = method.api.config.PLC_API_MAINTENANCE_PASSWORD

            if auth['AuthString'] != password:
                raise PLCAuthenticationFailure, "Maintenance account password verification failed"
        else:
            # Compare encrypted plaintext against encrypted password stored in the DB
            plaintext = auth['AuthString'].encode(method.api.encoding)
            password = person['password']


            # Protect against blank passwords in the DB
            if password is None or password[:12] == "" or \
                crypt.crypt(plaintext, password[:12]) != password:
                raise PLCAuthenticationFailure, "Password verification failed"

        if not set(person['roles']).intersection(method.roles):
  	    raise PLCAuthenticationFailure, "Not allowed to call method"

        method.caller = person

class PLEAuth(Auth):
    """
    External authentication scheme: PLE login
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'ple'", optional = False),
            'Username': Parameter(str, "PlanetLab Europe username, typically an e-mail address", optional = False),
            'AuthString': Parameter(str, "Authentication string, typically a password", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert auth.has_key('Username')

        # Check PLE authentication
        import xmlrpclib
        ple = xmlrpclib.ServerProxy('https://www.planet-lab.eu/PLCAPI/', allow_none=True)
        auth = {
            'AuthMethod': 'password',
            'Username': auth['Username'].lower(),
            'AuthString': auth['AuthString']
        }
        if not ple.AuthCheck(auth):
            raise PLCAuthenticationFailure, "No such PlanetLab Europe account"

        # System requires the person to be present in the local database
        persons = Persons(method.api, {'email': auth['Username'].lower()})
        if not persons:
            persons = ple.GetPersons(auth, {'email': auth['Username'].lower()}, ['first_name', 'last_name', 'title', 'email', 'phone', 'url', 'bio', 'roles', 'role_ids'])
            if not persons:
                raise PLCAuthenticationFailure, "No such PlanetLab Europe account"
            person_fields = persons[0]

            person_fields['enabled'] = True
            # XXX track origin ?
            # no password!
            person = Person(method.api, person_fields)
            person.sync()
        else:
            person = persons[0]

        roles = Roles(method.api, ['user'])
        if not roles:
            raise PLCInvalidArgument, "Invalid role 'user'"
        role = roles[0]
        if not 'role_ids' in person or role['role_id'] not in person['role_ids']:
            person.add_role(role)
            person['roles'].append('user')

        if not set(person['roles']).intersection(method.roles):
            raise PLCAuthenticationFailure, "Not allowed to call method"

        method.caller = person

class PLCAuth(Auth):
    """
    External authentication scheme: PLC login
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'ple'", optional = False),
            'Username': Parameter(str, "PlanetLab Central: username, typically an e-mail address", optional = False),
            'AuthString': Parameter(str, "Authentication string, typically a password", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert auth.has_key('Username')

        # Check PLC authentication
        import xmlrpclib
        plc = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/', allow_none=True)
        auth = {
            'AuthMethod': 'password',
            'Username': auth['Username'].lower(),
            'AuthString': auth['AuthString']
        }
        if not plc.AuthCheck(auth):
            raise PLCAuthenticationFailure, "No such PlanetLab Central account"

        # System requires the person to be present in the local database
        persons = Persons(method.api, {'email': auth['Username'].lower()})
        if not persons:
            persons = plc.GetPersons(auth, {'email': auth['Username'].lower()}, ['first_name', 'last_name', 'title', 'email', 'phone', 'url', 'bio', 'roles', 'role_ids'])
            if not persons:
                raise PLCAuthenticationFailure, "No such PlanetLab Central account"
            person_fields = persons[0]

            person_fields['enabled'] = True
            # XXX track origin ?
            # no password!
            person = Person(method.api, person_fields)
            person.sync()
        else:
            person = persons[0]

        roles = Roles(method.api, ['user'])
        if not roles:
            raise PLCInvalidArgument, "Invalid role 'user'"
        role = roles[0]
        if not 'role_ids' in person or role['role_id'] not in person['role_ids']:
            person.add_role(role)
            person['roles'].append('user')

        if not set(person['roles']).intersection(method.roles):
            raise PLCAuthenticationFailure, "Not allowed to call method"

        method.caller = person

class GuestAuth(Auth):
    # XXX equivalent to anonymous 
    """
    External authentication scheme: Guest login
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'ple'", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
	pass

class ManagedAuth(Auth):
    """
    Authentication on behalf of somebody else
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'ple'", optional = False),
            'AuthAdmin': Parameter(str, "", optional = False),
            'AuthString': Parameter(str, "Authentication string, typically a password", optional = False),
            'Username': Parameter(str, "username, typically an e-mail address", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.

        # XXX Do we need to add the person to the database

        # XXX Do we need to check method roles
        assert auth.has_key('AuthAdmin')
        assert auth.has_key('Username')

        if auth['AuthAdmin'] == method.api.config.MYSLICE_API_MAINTENANCE_USER:
            password = method.api.config.MYSLICE_API_MAINTENANCE_PASSWORD

            if auth['AuthString'] != password:
                raise PLCAuthenticationFailure, "Admin account password verification failed"
        else:
            raise PLCAuthenticationFailure, "Not allowed to login with managed accounts"

        # Get record (must be enabled)
        # XXX we can authenticate with either email or hrn !
        if '@' in auth['Username']:
            persons = Persons(method.api, {'email': auth['Username'].lower(), 'enabled': True, 'peer_id': None})
        else:
            persons = Persons(method.api, {'person_hrn': auth['Username'].lower(), 'enabled': True})
        # We create one since this is allowed by ManagedAuth by definition
        # System requires the person to be present in the local database
        if not persons:
            person_fields = {
                'enabled': True
            }
            if '@' in auth['Username']:
                person_fields['email'] = auth['Username']
            else:
                person_fields['person_hrn'] = auth['Username']
            # XXX track origin ?
            # no password!
            person = Person(method.api, person_fields)
            person.sync()
        else:
            if len(persons) != 1:
                raise PLCAuthenticationFailure, "No such account (PW)"
            else:
                person = persons[0]
        
        # Default role = user
        roles = Roles(method.api, ['user'])
        if not roles:
            raise PLCInvalidArgument, "Invalid role 'user'"
        role = roles[0]
        if not 'role_ids' in person or role['role_id'] not in person['role_ids']:
            person.add_role(role)

        if not set(person['roles']).intersection(method.roles):
            raise PLCAuthenticationFailure, "Not allowed to call method"

        method.caller = person

